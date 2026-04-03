(ns ibooks-migration.pipeline.workers.prepare-task
  (:require [ibooks-migration.book :refer [file-name file-extension book-exists?]]
            [ibooks-migration.db.migrator :as db-migrator]
            [ibooks-migration.pipeline.workers.core
             :as workers
             :refer [run-in-io-thread]]
            [clojure.core.async :as a]))

(defn ibooks-record->task [rec]
  (-> rec
      (assoc :path (:ZBKLIBRARYASSET/ZPATH rec))
      (dissoc :ZBKLIBRARYASSET/ZPATH)
      (assoc :guid (:ZBKLIBRARYASSET/ZASSETGUID rec))
      (dissoc :ZBKLIBRARYASSET/ZASSETGUID)))

(defn enrich-by-extension [tmp-root {:keys [path] :as task}]
  (let [extension (-> path file-name file-extension)]
    (case extension
      "ibooks" (assoc task
                      :format :ibooks
                      :src-path (format "%s/%s" tmp-root (file-name path)))

      "epub"   (assoc task
                      :format :epub
                      :src-path (format "%s/%s" tmp-root (file-name path)))
      "pdf"    (assoc task
                      :format :pdf
                      :src-path path))))

(def allowed-transitions
  {:row-retrieving #{:checking-task-already-done :stopping :failed}
   :checking-task-already-done #{:checking-path-exists :stopping}
   :checking-path-exists #{:task-forwarding :stopping :task-error-forwarding}
   :task-error-forwarding #{:row-retrieving :stopping :failed}
   :stopping #{:stopped}
   :stopped #{}
   :failed #{}})

(def transit (partial workers/transit allowed-transitions))

;; begin -- cmd-ch handling
(defmulti handle-cmd (fn [kind _] kind))

(defmethod handle-cmd :stop [_ {:keys [evt-ch] :as s}]
  (a/>! evt-ch {:kind :stopping})
  (transit s :stopping))
;; end   -- cmd-ch handling

;; -> get row and -> task and enrich, drop ipub task
;; -> check task done - just drop
;; -> check path exists - send message to task-error-ch
;; -> 

(defmulti handle-state :state)

(defmethod handle-state :row-retrieving [{:keys [cmd-ch in-ch tmp-root] :as s}]
  (let [[v ch] (a/alts! [cmd-ch in-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (= ch in-ch)

      (if (nil? v)
        (-> s
            (set-in-ch-closed-error {})
            (transit :failed))
        (let [enrich-by-extension (partial enrich-by-extension tmp-root)
              {:keys [format] :as task} (-> v ibooks-record->task enrich-by-extension)
              drop-formats #{:ibooks}]
          (if (contains? drop-formats format)
            (transit s :row-retrieving)
            (-> s
                (assoc :task task)
                (transit :checking-task-already-done))))))))

(defn set-check-task-done-error [{:keys [state worker] :as s} opts]
  (-> s
      (assoc-in [:error :state] state)
      (assoc-in [:error :action] :check-task-already-done)
      (assoc-in [:error :worker] worker)
      (update-in [:error] (partial merge opts))))

(defmethod handle-state :checking-task-already-done [{:keys [cmd-ch db task] :as s}]
  (let [task-done-check-ch (run-in-io-thread :nil db-migrator/task-done? db task)
        [v ch] (a/alts! [cmd-ch task-done-check-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch task-done-check-ch) (instance? Throwable v))
      ;; go to failed
      (-> s
          (set-check-task-done-error {:exception v})
          (transit :failed))

      (and (= ch task-done-check-ch) (true? v))
      ;; task already done, drop
      (-> s
          (dissoc :task)
          (transit :row-retrieving))

      (and (= ch task-done-check-ch) (false? v))
      ;; task was never performed, continue
      (transit :checking-path-exists))))

(defn task-set-book-exists-check-failed [task e]
  (assoc task :error {:exception e
                      :type :book-exists-thrown-exception}))


(defmethod handle-state :checking-path-exists [{:keys [cmd-ch task] :as s}]
  (let [{:keys [path]} task
        book-exists-ch (run-in-io-thread :nil book-exists? path)
        [v ch] (a/alts! [cmd-ch book-exists-ch] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch book-exists-ch) (instance? Throwable v))
      ;; we got path from ibooks db. if it does not exists - probably reason
      ;; notify about task error
      (-> s
          (update-in [:task :error] #(task-set-book-exists-check-failed %1 v))
          (transit :task-error-forwarding))

      (and (= ch book-exists-ch) (true? v))
      ;; continue
      (transit s :task-forwarding)

      (and (= ch book-exists-ch) (false? v))
      ;; notify about task error
      (-> s
          (assoc-in [:task :error] {:type :path-does-not-exists})
          (transit :task-error-forwarding)))))

(defmethod handle-state :task-forwarding [{:keys [cmd-ch out-ch task] :as s}]
  (let [[v ch] (a/alts! [cmd-ch [out-ch task]] :priority true)]
    (cond
      (= ch cmd-ch) (handle-cmd s v)

      (and (= ch out-ch) (false? v))
      (-> s
          (workers/set-out-ch-closed-error {})
          (transit :failed))

      (and (= ch out-ch) (true? v))
      (transit s :row-retrieving))))

(defn set-worker-type [s]
  (assoc-in s [:worker :type] :prepare-task))


(defn start [opts]
  (let [initial-state (-> opts
                          (merge {:state :row-retrieving})
                          set-worker-type)]
    (a/go-loop [state initial-state]
      (when-let [next-state (handle-state state)]
        (recur next-state)))))
