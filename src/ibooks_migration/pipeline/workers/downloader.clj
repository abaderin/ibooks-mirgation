(ns ibooks-migration.pipeline.workers.downloader
  (:require [clojure.core.async :as a]
            [ibooks-migration.pipeline.workers.core
             :as workers
             :refer [run-in-io-thread]]
            [ibooks-migration.icloud :refer [brctl-download dataless?]]))

(def allowed-transitions
  {:receiving-task #{:starting-download :stopping :failed}
   :starting-download #{:waiting-download-completed :stopping :failed}
   :checking-download-completed #{:waiting-download-completed}
   :waiting-download-completed #{:waiting-download-completed
                                 :forwarding-task
                                 :stopping
                                 :failed}
   :forwarding-task #{:starting-download :stopping :failed}
   :stopping #{:stopped}
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

(defmethod handle-state :starting-download [{:keys [cmd-ch task] :as s}]
  (let [{:keys [path]} task
        download-result-ch (run-in-io-thread :done brctl-download path)
        [v ch] (a/alts! [cmd-ch download-result-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd v s)

      (and (= ch download-result-ch) (instance? Throwable v))
      (-> s
          (assoc :error v)
          (transit :failed))

      (and (= ch download-result-ch) (= :done v))
      (transit s :waiting-download-completed))))

(defmethod handle-state :checking-download-completed [{:keys [task cmd-ch] :as s}]
  (let [{:keys [path]} task
        download-not-completed-ch (run-in-io-thread :nil dataless? path)
        [v ch] (a/alts! [cmd-ch download-not-completed-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch download-not-completed-ch) (instance? Throwable v))
      (-> s
          (assoc :error v)
          (transit :failed))

      ;; dataless = false, means path has been downloaded
      (and (= ch download-not-completed-ch) (false? v))
      (transit s :forwarding-task)

      ;; dataless = true -> path is not ready
      (and (= ch download-not-completed-ch) (true? v))
      (transit s :waiting-download-completed))))

(defmethod handle-state :waiting-download-completed [{:keys [check-timeout-ms cmd-ch] :as s}]
  (let [timeout-ch (a/timeout check-timeout-ms)
        [v ch] (a/alts! [cmd-ch timeout-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (= ch timeout-ch)
      (transit s :checking-download-completed))))

(defmethod handle-state :forwarding-task [{:keys [cmd-ch task out-ch] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [out-ch task]] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch out-ch) (true? v))
      (-> s
          (dissoc :task)
          (transit s :receiving-task))

      (and (= ch out-ch) (false? v))
      (-> s
          (workers/set-out-ch-closed-error {})
          (transit :failed)))))

(defmethod handle-state :stopping [s]
  (transit s :stopped))

(defmethod handle-state :stopped [{:keys [evt-ch]}]
  (a/>! evt-ch {:kind :stopped})
  nil)

(defmethod handle-state :failed [{:keys [evt-ch error]}]
  (a/>! evt-ch {:kind :failed :error error})
  nil)
