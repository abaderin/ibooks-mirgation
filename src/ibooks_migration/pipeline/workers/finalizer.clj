(ns ibooks-migration.pipeline.workers.finalizer
  (:require [ibooks-migration.pipeline.workers.core
             :as workers
             :refer [run-in-io-thread
                     set-in-ch-closed-error]]
            [clojure.core.async :as a]
            [ibooks-migration.db.migrator :as db-migrator]))



(def allowed-transitions
  {:receiving-task #{:marking-task-done :stopping :failed}
   :marking-task-done #{:receiving-task :stopping :failed}
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

(defmethod handle-state :receiving-task [{:keys [in-ch cmd-ch] :as s}]
  (let [[v ch] (a/alts! [cmd-ch in-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch in-ch) (nil? v))
      (-> s
          set-in-ch-closed-error
          (transit :failed))

      (= ch in-ch)
      (-> s
          (assoc :task v)
          (transit :marking-task-done)))))

(defn set-setting-task-done-db-error
  ([s] (set-setting-task-done-db-error s {}))
  ([{:keys [state worker] :as s} opts]
   (-> s
       (assoc-in [:error :state] state)
       (assoc-in [:error :action] :db-setting-task-done)
       (assoc-in [:error :worker] worker)
       (update-in [:error] (partial merge opts)))))

(defmethod handle-state :marking-task-done [{:keys [cmd-ch task db] :as s}]
  (let [task-mark-done-ch (run-in-io-thread :nil db-migrator/task-done! db task)
        [v ch] (a/alts! [cmd-ch task-mark-done-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch task-mark-done-ch) (instance? Throwable v))
      ;; something gone bad
      (-> s
          set-setting-task-done-db-error
          (transit :failed))

      ;; ok
      (and (= ch task-mark-done-ch) (int? v))
      (-> s
          (dissoc :task)
          (transit :receiving-task)))))


(defmethod handle-state :stopping [s]
  (transit s :stopped))

(defmethod handle-state :stopped [_]
  nil)

(defmethod handle-state :failed [_]
  nil)

(defn set-worker-type [s]
  (assoc-in s [:worker :type] :finalizer))

(defn start [opts]
  (let [initial-state (-> opts
                          (merge {:state :receiving-task})
                          set-worker-type)]
    (a/go-loop [state initial-state]
      (when-let [next-state (handle-state state)]
        (recur next-state)))))
