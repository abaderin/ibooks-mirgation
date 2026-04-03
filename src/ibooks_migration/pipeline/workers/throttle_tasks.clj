(ns ibooks-migration.pipeline.workers.throttle-tasks
  (:require [clojure.core.async :as a]
            [clojure.set :refer [difference]]
            [clojure.pprint :refer [pprint]]
            [ibooks-migration.pipeline.workers.core
             :as workers
             :refer [run-in-io-thread]]))

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

(def transit (partial workers/transit allowed-transitions))

;; begin -- cmd-ch handling
(defmulti handle-cmd (fn [kind _] kind))

(defmethod handle-cmd :stop [_ {:keys [evt-ch] :as state}]
  (a/>! evt-ch {:kind :stopping})
  (transit state :stopping))
;; end   -- cmd-ch handling

(defmulti handle-state :state)

(defmethod handle-state :task-waiting [{:keys [cmd-ch in-ch] :as s}]
  (let [[v ch] (a/alts! [cmd-ch in-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v s)

      (= ch in-ch)

      (if (nil? v)
        ;; task is nil - channel closed. reasanoble to free resources and do
        ;; green worker shutdown
        (transit s :stopping)
        ;; if task is not nil
        (-> s
            (assoc :task v)
            (transit :size-calculation))))))

(defn task-set-size [size task]
  (let [size (case (-> task :format)
               :epub (* 2 size)
               size)]
    (assoc task :size size)))

(defmethod handle-state :size-calculation [{:keys [cmd-ch] :as s}]
  (let [path (-> s :task :path)
        size-fn (get-in s [:deps :size-fn])
        calculated-size-ch (run-in-io-thread :nil size-fn path)
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
      (transit s :disk-space-release-waiting)

      :else
      (-> s
          (assoc :current-disk-space-usage (+ current-disk-space-usage task-size))
          (transit :task-forwarding)))))

(defmethod handle-state :disk-space-release-waiting [{:keys [disk-space-release-ch cmd-ch] :as s}]
  (let [[v ch] (a/alts! [cmd-ch disk-space-release-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v s)

      (= ch disk-space-release-ch)

      (-> s
          (assoc :current-disk-space-usage (-> s :current-disk-space-usage (- v)))
          (transit :disk-space-reservation)))))

(defmethod handle-state :task-forwarding [{:keys [cmd-ch out-ch task] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [out-ch task]] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v s)

      (= ch out-ch) (if (true? v)
                      (transit s :task-waiting)
                      (-> s
                          (assoc :reason {:state :task-forwarding
                                          :message "out-ch is closed"})
                          (transit :failed))))))

(defmethod handle-state :task-error-forwarding [{:keys [cmd-ch task-error-ch task] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [task-error-ch task]] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v s)

      (= ch task-error-ch) (if (true? v)
                             (transit s :task-waiting)
                             (-> s
                                 (assoc :reason {:state :task-error-forwarding
                                                 :message "task-error-ch is closed"})
                                 (transit :failed))))))

(defmethod handle-state :stopping [s]
  (throw (ex-info "Not implemented" {}))
  (transit s :stopped))

(defmethod handle-state :stopped [{:keys [evt-ch]}]
  (a/>! evt-ch {:kind :stopped})
  nil)

(defmethod handle-state :failed [{:keys [evt-ch]}]
  (a/>! evt-ch {:kind :failed})
  nil)

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
                              :current-disk-space-usage 0
                              :worker :throttle-tasks})]
    (a/go-loop [state initial-state]
      (when-let [next-state (handle-state state)]
        (recur next-state)))))
