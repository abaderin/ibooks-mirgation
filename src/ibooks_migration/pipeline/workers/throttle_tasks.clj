(ns ibooks-migration.pipeline.workers.throttle-tasks
  (:require [clojure.core.async :as a]
            [ibooks-migration.book :refer [fs-size]]
            [clojure.set :refer [difference]]
            [clojure.pprint :refer [pprint]]))

;; begin -- cmd-ch handling
(defmulti handle-cmd (fn [kind _] kind))

(defmethod handle-cmd :stop [_ {:keys [evt-ch] :as state}]
  (a/>! evt-ch {:kind :stopping})
  (assoc state :state :stopping))
;; end   -- cmd-ch handling

(def allowed-transitions
  {:task-waiting               #{:size-calculation :stopping}
   :size-calculation           #{:disk-space-reservation :task-error-forwarding :stopping}
   :disk-space-reservation     #{:task-forwarding :disk-space-release-waiting}
   :disk-space-release-waiting #{:disk-space-reservation :stopping}
   :task-forwarding            #{:stopping :task-waiting}
   :task-error-forwarding      #{:task-waiting :stopping}
   :stopping                   #{:stopped}
   :stopped                    #{}
   :failed                     #{}})

(defn terminal-state? [allowed-transitions state]
  (let [terminal-states (->> allowed-transitions
                             keys
                             (filter #(empty? (% allowed-transitions)))
                             set)]
    (contains? terminal-states state)))

(defn valid-transitions-map? [transitions-map]
  (let [source-states (set (keys transitions-map))]
    (every?
     (fn [[_ target-states]]
       (every? source-states target-states))
     transitions-map)))

(assert (valid-transitions-map? allowed-transitions) "allowed-transitions in incorrect state")

(defn transit [{:keys [state] :as s} to-state]
  (assert (keyword? (-> allowed-transitions state to-state))
          (format "incorrent-transition %s -> %s" state to-state))
  (assoc s :state to-state))

(defmulti handle-state :state)

(defmethod handle-state :task-waiting [{:keys [cmd-ch in-ch evt-ch] :as state}]
  (let [[v ch] (a/alts! [cmd-ch in-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v state)

      (= ch in-ch) (assoc state
                          :state :calculating-size
                          :task v))))

(defn task-set-size [size task]
  (let [size (case (-> task :format)
               :epub (* 2 size)
               size)]
    (assoc task :size size)))

(defmethod handle-state :size-calculation [{:keys [cmd-ch] :as s}]
  (let [path (-> s :task :path)
        calculated-size-ch (a/go
                             (try
                               (fs-size path)
                               (catch Throwable e e)))
        [v ch] (a/alts! [cmd-ch calculated-size-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v s)

      (= ch calculated-size-ch)

      (cond
        (instance? Throwable v) (-> s
                                    (assoc-in [:task :error] {:step :size-calculation
                                                              :details v})
                                    (transit :task-error-forwarding))

        :else (-> s
                  (update :task (partial task-set-size v))
                  (transit :disk-space-reservation))))))

(defmethod handle-state :disk-space-reservation [{:keys [max-disk-space-usage current-disk-space-usage] :as s}]
  (let [task-size (-> s :task :size)]
    (cond
      ;; if task size > max-disk-space usage we have nothing to do here
      ;; it is imposible to perform it at all
      (> task-size max-disk-space-usage) (-> s
                                             (assoc-in [:task :error] {:step :disk-space-reservation
                                                                       :details {:max-disk-space-usage max-disk-space-usage
                                                                                 :task-size task-size
                                                                                 :message "task-size > max-disk-space-usage, task will never be able to be performed"}})
                                             (transit :task-error-forwarding))

      ;; if we have not enough free space - wait till it be available
      (> (+ task-size current-disk-space-usage) max-disk-space-usage)
      (assoc :state :disk-space-release-waiting)

      :else
      (-> s
          (assoc :current-disk-space-usage (+ current-disk-space-usage task-size))
          (assoc :state :task-forwarding)))))

(defmethod handle-state :disk-space-release-waiting [{:keys [disk-space-release-ch cmd-ch] :as s}]
  (let [[v ch] (a/alts! [cmd-ch disk-space-release-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v s)

      (= ch disk-space-release-ch)

      (assoc s :current-disk-space-usage (-> s :current-disk-space-usage (- v))))))

(defmethod handle-state :task-forwarding [{:keys [cmd-ch out-ch task] :as s}]
  (let [task-forwarded-ch (a/go (a/>! out-ch task))
        [v ch] (a/alts! [cmd-ch task-forwarded-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v s)

      (= ch task-forwarded-ch) (transit s :task-waiting))))

(defmethod handle-state :task-error-forwarding [{:keys [cmd-ch task-error-ch task] :as s}]
  (let [task-error-forwarded-ch (a/go (a/>! task-error-ch task))
        [v ch] (a/alts! [cmd-ch task-error-forwarded-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v s)

      (= ch task-error-ch) (transit s :task-waiting))))

(defmethod handle-state :stopping [s]
  (throw (ex-info "Not implemented" {}))
  (transit s :stopped))

(defmethod handle-state :stopped [{:keys [evt-ch]}]
  (a/>! evt-ch {:kind :stopped}))

(defmethod handle-state :failed [{:keys [evt-ch]}]
  (a/>! evt-ch {:kind :failed}))

(defn throttle-tasks-worker
  [{:keys [max-disk-space-usage
           cmd-ch
           in-ch
           out-ch
           evt-ch
           space-free-up-ch
           disk-space-release-ch
           task-error-ch] :as params}]
  (let [initial-state (merge params
                             {:state :task-waiting
                              :current-disk-space-usage 0})]
    (a/go-loop [state initial-state]
      (when-not (terminal-state? allowed-transitions (:state state))
        (recur (handle-state state))))))

(let [declared-states (-> allowed-transitions keys set)
      handled-states (-> handle-state methods keys set)
      unhandled-states (difference declared-states handled-states)
      undeclared-states (difference handled-states declared-states)]
  (when-not (empty? unhandled-states)
    (pprint unhandled-states))
  (assert (empty? unhandled-states) "You have states which has not been implemented, dumb ass")
  (when-not (empty? undeclared-states)
    (pprint undeclared-states))
  (assert (empty? undeclared-states) "You have implementations for states which are not used, dumb ass"))
