(ns ibooks-migration.pipeline.workers.uploader
  (:require [clojure.core.async :as a]
            [ibooks-migration.pipeline.workers.core
             :as workers
             :refer [run-in-io-thread]]
            [ibooks-migration.book :refer [md5sum ssh-md5sum]]))

(def allowed-transitions
  {:receiving-task #{:uploading-book :stopping :failed}
   :uploading-book #{:calculating-local-md5 :stopping :failed}
   :calculating-local-md5 #{:calculating-remote-md5 :stopping :failed}
   :calculating-remote-md5 #{:forwarding-task :forwarding-task-error :stopping :failed}
   :forwarding-task #{:receiving-task :stopping :failed}
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

(defmethod handle-state :receiving-task [{:keys [cmd-ch in-ch] :as s}]
  (let [[v ch] (a/alts! [cmd-ch in-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch in-ch) (nil? v))
      (-> s
          (workers/set-in-ch-closed-error {})
          (transit :failed))

      (= ch in-ch)
      (-> s
          (assoc :task v)
          (transit :uploading-book)))))

(defn set-upload-book-ch-closed-error [{:keys [state worker] :as s} opts]
  (-> s
      (assoc-in [:error :state] state)
      (assoc-in [:error :action] :upload-book-ch-pulling)
      (assoc-in [:error :worker] worker)
      (update-in [:error] (partial merge opts))))

(defmethod handle-state :uploading-book [{:keys [cmd-ch task upload-fn] :as s}]
  (let [{:keys [src-path]} task
        upload-fn (partial upload-fn src-path)
        upload-book-ch (run-in-io-thread :nil upload-fn)
        [v ch] (a/alts! [cmd-ch upload-book-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch upload-book-ch) (nil? v))
      (-> s
          (set-upload-book-ch-closed-error {})
          (transit :failed))

      (= ch upload-book-ch)
      (transit s :calculating-local-md5))))

(defn set-local-md5-ch-closed-error [{:keys [state worker] :as s} opts]
  (-> s
      (assoc-in [:error :state] state)
      (assoc-in [:error :action] :local-md5-ch-pulling)
      (assoc-in [:error :worker] worker)
      (update-in [:error] (partial merge opts))))

(defmethod handle-state :calculating-local-md5 [{:keys [cmd-ch task] :as s}]
  (let [{:keys [src-path]} task
        md5-ch (run-in-io-thread :nil md5sum src-path)
        [v ch] (a/alts! [cmd-ch md5-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch md5-ch) (nil? v))
      (-> s
          (set-local-md5-ch-closed-error {})
          (transit :failed))

      (and (= ch md5-ch) (int? v))
      (-> s
          (assoc-in [:task :local-md5] v)
          (transit :calculating-remote-md5)))))

(defn set-remote-md5-ch-closed-error [{:keys [state worker] :as s} opts]
  (-> s
      (assoc-in [:error :state] state)

      (assoc-in [:error :action] :remote-md5-ch-pulling)
      (assoc-in [:error :worker] worker)
      (update-in [:error] (partial merge opts))))

(defn task-set-md5-doesnt-matches [task e]
  (assoc task :error {:exception e
                      :type :md5-doesnt-matches}))

(defmethod handle-state :calculating-remote-md5 [{:keys [cmd-ch calc-fn task] :as s}]
  (let [{:keys [src-path]} task
        calc-fn (partial calc-fn src-path)
        remote-md5-ch (run-in-io-thread :nil calc-fn)
        [v ch] (a/alts! [cmd-ch remote-md5-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch remote-md5-ch) (nil? v))
      (-> s
          (set-remote-md5-ch-closed-error {})
          (transit :failed))

      (and (= ch remote-md5-ch) (int? v))
      (let [remote-md5 v
            {:keys [local-md5]} task
            s (assoc-in s [:task :remote-md5] remote-md5)]
        (if (= local-md5 remote-md5)
          (transit s :forwarding-task)
          (-> s
              (update-in [:task :error] #(task-set-md5-doesnt-matches %1 v))
              (transit s :forwarding-task-error)))))))

(defmethod handle-state :forwarding-task [{:keys [cmd-ch out-ch task] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [out-ch task]] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch out-ch) (false? v))
      (-> s
          (workers/set-out-ch-closed-error {})
          (transit :failed))

      (and (= ch out-ch) (true? v))
      (-> s
          (dissoc :task)
          (transit :receiving-task)))))

(defmethod handle-state :forwarding-task-error [{:keys [cmd-ch evt-ch task] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [evt-ch task]] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch evt-ch) (false? v))
      (-> s
          (workers/set-evt-ch-closed-error {})
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
  (assoc-in s [:worker :type] :uploader))

(defn start [opts]
  (let [initial-state (-> opts
                          (merge {:state :receiving-task})
                          set-worker-type)]
    (a/go-loop [state initial-state]
      (when-let [next-state (handle-state state)]
        (recur next-state)))))

