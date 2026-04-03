(ns ibooks-migration.pipeline.workers.cleaner
  (:require [ibooks-migration.pipeline.workers.core
             :as workers
             :refer [set-out-ch-closed-error
                     set-in-ch-closed-error
                     set-evt-ch-closed-error
                     run-in-io-thread]]
            [clojure.core.async :as a]
            [ibooks-migration.book :refer [rm]]
            [ibooks-migration.icloud :refer [brctl-evict dataless?]]))

(def allowed-transitions
  {:receiving-task #{:cleaning-epub :starting-icloud-eviction :stopping :failed}
   :cleaning-epub #{:starting-icloud-eviction :forwarding-task-error :stopping :failed}
   :starting-icloud-eviction #{:checking-icloud-eviction-completed :stopping :failed}
   :checking-icloud-eviction-completed #{:waiting-icloud-eviction-completed :stopping :failed}
   :waiting-icloud-eviction-completed  #{:forwarding-task :stopping :failed}
   :forwarding-task #{:receiving-task}
   :stopping #{:stopped :failed}
   :stopped #{}
   :failed #{}})

(def transit (partial workers/transit allowed-transitions))

;; begin -- cmd-ch handling
(defmulti handle-cmd (fn [kind _] kind))

(defmethod handle-cmd :stop [_ {:keys [evt-ch] :as state}]
  (a/>! evt-ch {:kind :stopping})
  (transit state :stopping))
;; end   -- cmd-ch handling

(defmulti handle-state :state)

(defmethod handle-state :receiving-task [{:keys [cmd-ch in-ch] :as s}]
  (let [[v ch] (a/alts! [cmd-ch in-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch in-ch) (nil? v))
      (-> s
          set-in-ch-closed-error
          (transit :failed))

      (and (= ch in-ch))
      (let [task v
            transit-to (if (= :epub (:format task))
                         :cleaning-epub
                         :starting-icloud-eviction)]
        (-> s
            (assoc :task v)
            (transit transit-to))))))

(defn task-set-epub-remove-failed [task e]
  (assoc task :error {:exception e
                      :type :epub-remove-failed}))

(defmethod handle-state :cleaning-epub [{:keys [cmd-ch task] :as s}]
  (let [{:keys [src-path]} task
        remove-result-ch (run-in-io-thread :done rm src-path)
        [v ch] (a/alts! [cmd-ch remove-result-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch remove-result-ch) (= :done v))
      (transit s :forwarding-task)

      (and (= ch remove-result-ch) (instance? Throwable v))
      (-> s
          (update-in [:task :error] #(task-set-epub-remove-failed % v))
          (transit :forwarding-task-error)))))

(defn task-set-brctl-evict-failed [state e]
  (assoc-in state [:task :error] {:exception e
                            :type :brctl-evict-failed}))

(defmethod handle-state :starting-icloud-eviction [{:keys [cmd-ch task] :as s}]
  (let [{:keys [path]} task
        eviction-result-ch (run-in-io-thread :done brctl-evict path)
        [v ch] (a/alts! [cmd-ch eviction-result-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch eviction-result-ch) (instance? Throwable v))
      ;; task error
      (-> s
          (task-set-brctl-evict-failed v)
          (transit :forwarding-task-error)

      (and (= ch eviction-result-ch) (= :done v))
      (transit s :checking-icloud-eviction-completed)))))

(defmethod handle-state :checking-icloud-eviction-completed [{:keys [cmd-ch task] :as s}]
  (let [{:keys [path]} task
        eviction-completed-ch (run-in-io-thread :nil dataless? path)
        [v ch] (a/alts! [cmd-ch eviction-completed-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd)

      ;; path evicted
      (and (= ch eviction-completed-ch) (true? v))
      (transit s :forwarding-task)
      ;; still not evicted
      (and (= ch eviction-completed-ch) (false? v))
      (transit s :waiting-icloud-eviction-completed))))

(defmethod handle-state :waiting-icloud-eviction-completed [{:keys [cmd-ch check-timeout-ms] :as s}]
  (let [timeout-ch (a/timeout check-timeout-ms)
        [v ch] (a/alts! [cmd-ch timeout-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (= ch timeout-ch)
      (transit s :checking-icloud-eviction-completed))))

(defmethod handle-state :forwarding-task [{:keys [cmd-ch out-ch task] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [out-ch task]] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch out-ch) (true? v))
      (-> s
          (dissoc :task)
          (transit :receiving-task))

      (and (= ch out-ch) (false? v))
      (-> s
          set-out-ch-closed-error
          (transit :failed)))))

(defmethod handle-state :forwarding-task-error [{:keys [cmd-ch evt-ch task] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [evt-ch task]])]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch evt-ch) (false? evt-ch))
      (-> s
          set-evt-ch-closed-error
          (transit :failed))

      (and (= ch evt-ch) (true? v))
      (-> s
          (dissoc :task)
          (transit :receiving-task)))))

(defmethod handle-state :stopping [s]
  (transit s :stopped))

(defmethod handle-state :stopped [_]
  nil)

(defmethod handle-state :stopped [_]
  nil)

(defn set-worker-type [s]
  (assoc-in s [:worker :type] :cleaner))

(defn start [opts]
  (let [initial-state (-> opts
                          (merge {:state :receiving-task})
                          set-worker-type)]
    (a/go-loop [state initial-state]
      (when-let [next-state (handle-state state)]
        (recur next-state)))))

