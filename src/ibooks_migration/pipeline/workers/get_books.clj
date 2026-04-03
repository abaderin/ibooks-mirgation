(ns ibooks-migration.pipeline.workers.get-books
  (:require [clojure.core.async :as a]
            [ibooks-migration.db.ibooks :as db-ibooks]
            [ibooks-migration.pipeline.workers.core
             :as workers
             :refer [run-in-io-thread]]))

(def allowed-transitions
  {:row-retrieving #{:row-forwarding :failed :stopping}
   :row-forwarding #{:row-retrieving :failed :stopping}
   :stopping #{}
   :stopped #{}})

(def transit (partial workers/transit allowed-transitions))

;; begin -- cmd-ch handling
(defmulti handle-cmd (fn [kind _] kind))

(defmethod handle-cmd :stop [_ {:keys [evt-ch] :as s}]
  (a/>! evt-ch {:kind :stopping})
  (transit s :stopping))
;; end   -- cmd-ch handling

(defmulti handle-state :state)

(defn set-get-next-book-call-error [{:keys [state worker] :as s} opts]
    (-> s
        (assoc-in [:error :state] state)
        (assoc-in [:error :action] :get-next-book-call)
        (assoc-in [:error :worker] worker)
        (update-in [:error] (partial merge opts))))

(defmethod handle-state :row-retrieving [{:keys [ds cmd-ch task] :as s}]
  (let [row-ch (run-in-io-thread :eot db-ibooks/get-next-book ds task)
        [v ch] (a/alts! [cmd-ch row-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (= ch row-ch)

      (cond
        (instance? Throwable v)
        (-> s
            (set-get-next-book-call-error {:exception v})
            (transit s :failed))

        (= :eot v)
        (-> s
            (transit s :stopping))

        :else
        (-> s
            (assoc :task v)
            (transit s :row-forwarding))))))

(defmethod handle-state :row-forwarding [{:keys [cmd-ch cursor out-ch] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [out-ch cursor]] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch out-ch) (true? v))
      (transit s :row-retrieving)

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

(defn set-worker-type [s]
  (assoc-in s [:worker :type] :get-books))

(defn start [opts]
  (let [initial-state (-> opts
                          (merge {:state :row-retrieving
                                  :task {}})
                          set-worker-type)]
    (a/go-loop [state initial-state]
      (when-let [next-state (handle-state state)]
        (recur next-state)))))
