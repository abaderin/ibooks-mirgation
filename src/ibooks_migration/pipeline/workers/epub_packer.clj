(ns ibooks-migration.pipeline.workers.epub-packer
  (:require [ibooks-migration.pipeline.workers.core
             :as workers
             :refer [run-in-io-thread]]
            [ibooks-migration.epub :refer [pack-epub!]]
            [clojure.core.async :as a]))

(def allowed-transitions
  {:receiving-task #{:packing-epub :stopping :failed}
   :packing-epub #{:forwarding-task :stopping :failed}
   :forwarding-task #{:receiving-task :stopping :failed}
   :stopping #{:stopped}
   :failed #{}
   :stopped #{}})

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
          (workers/set-in-ch-closed-error {})
          (transit :failed))

      (and (= ch in-ch) (not (nil? v)))
      (let [{:keys [format]} v
            s (assoc s :task v)]
        (if (= :epub format)
          (transit s :packing-epub)
          (transit s :forwarding-task))))))


(defmethod handle-state :packing-epub [{:keys [cmd-ch task] :as s}]
  (let [{:keys [path src-path]} task
        pack-epub-result-ch (run-in-io-thread :nil pack-epub! path src-path)
        [v ch] (a/alts! [cmd-ch pack-epub-result-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch pack-epub-result-ch) (string? v))
      (transit s :forwarding-task)

      (and (= ch pack-epub-result-ch) (instance? Throwable v))
      (-> s
          (assoc :error v)
          (transit :failed)))))

(defmethod handle-state :forwarding-task [{:keys [cmd-ch out-ch task] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [out-ch task]] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch out-ch) (true? v))
      (transit s :receiving-task)

      (and (= ch out-ch) (false? v))
      (-> s
          (workers/set-out-ch-closed-error {})
          (transit :failed)))))

(defmethod handle-state :stopping [s]
  (transit s :stopped))

(defmethod handle-state :stopped [s]
  nil)

(defmethod handle-state :failed [s]
  nil)

(defn set-worker-type [s]
  (assoc-in s [:worker :type] :uploader))

(defn start [opts]
  (let [initial-state (-> opts
                          (merge {:state :receiving-task})
                          set-worker-type)]
    (a/go-loop [state initial-state]
      (when-let [next-state (handle-state state)]
        (recur next-state)))))
