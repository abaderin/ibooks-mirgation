(ns ibooks-migration.worker.core
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as s]
            [ibooks-migration.book :as book]
            [ibooks-migration.db.ibooks :as ibooks-db]
            [ibooks-migration.db.migrator :as migrator-db]
            [ibooks-migration.epub :as epub]
            [ibooks-migration.icloud.core :as icloud]
            [ibooks-migration.self :as self]
            [ibooks-migration.task :as task]))

(def ^:private command-poll-ms 100)
(def ^:private default-buffer-size 64)

(def ^:private worker-definitions
  {:worker/get-books {:scale 1 :handler :source}
   :worker/task-preparation {:scale 1 :handler :task-preparation}
   :worker/task-throttler {:scale 1 :handler :task-throttler}
   :worker/downloader {:scale 8 :handler :downloader}
   :worker/epub-packer {:scale 2 :handler :epub-packer}
   :worker/uploader {:scale 8 :handler :uploader}
   :worker/cleaner {:scale 8 :handler :cleaner}
   :worker/finalizer {:scale 1 :handler :finalizer}})

(defn shell-send-book-fn [host remote-root]
  (fn [{:keys [src-path]}]
    (let [remote-root (s/replace remote-root #"/+$" "")
          remote-path (format "%s/%s" remote-root (book/file-name src-path))
          target (format "%s:%s" host remote-path)
          {:keys [err exit] :as sh-result} (sh "scp" src-path target)]
      (when (not= exit 0)
        (throw (ex-info err (merge {:src-path src-path
                                    :remote-path remote-path}
                                   sh-result))))
      remote-path)))

(defn default-pipeline
  [{:keys [ibooks-db migrator-db max-disk-space-usage remote-host send-book-fn workers]}]
  (let [worker-overrides (or workers {})]
    {:worker/get-books (merge {:db ibooks-db}
                              (get worker-overrides :worker/get-books))
     :worker/task-preparation (merge {:input (self/ref :worker/get-books)
                                      :db migrator-db}
                                     (get worker-overrides :worker/task-preparation))
     :worker/task-throttler (merge {:input (self/ref :worker/task-preparation)
                                    :max-disk-space-usage max-disk-space-usage}
                                   (get worker-overrides :worker/task-throttler))
     :worker/downloader (merge {:input (self/ref :worker/task-throttler)}
                               (get worker-overrides :worker/downloader))
     :worker/epub-packer (merge {:input (self/ref :worker/downloader)}
                                (get worker-overrides :worker/epub-packer))
     :worker/uploader (merge {:input (self/ref :worker/epub-packer)
                              :remote-host remote-host
                              :send-book-fn send-book-fn}
                             (get worker-overrides :worker/uploader))
     :worker/cleaner (merge {:input (self/ref :worker/uploader)}
                            (get worker-overrides :worker/cleaner))
     :worker/finalizer (merge {:input (self/ref :worker/cleaner)
                               :db migrator-db}
                              (get worker-overrides :worker/finalizer))}))

(defn- emit-event!! [events-ch event]
  (when events-ch
    (async/>!! events-ch (assoc event :timestamp (System/currentTimeMillis)))))

(defn- take-input!! [command-ch input-ch]
  (let [[value port] (async/alts!! [command-ch input-ch] :priority true)]
    (cond
      (= port command-ch) {:state :stop}
      (and (= port input-ch) (nil? value)) {:state :input-closed}
      :else {:state :value :value value})))

(defn- put-output!! [command-ch output-ch value]
  (if-not output-ch
    {:state :ok}
    (let [[put-result port] (async/alts!! [command-ch [output-ch value]] :priority true)]
      (cond
        (= port command-ch) {:state :stop}
        (false? put-result) {:state :output-closed}
        :else {:state :ok}))))

(defn- run-with-command!! [command-ch thunk]
  (let [result-ch (async/promise-chan)
        job (future
              (try
                (async/>!! result-ch {:status :ok
                                      :value (thunk)})
                (catch Throwable t
                  (async/>!! result-ch {:status :error
                                        :error t}))))]
    (loop []
      (let [[value port] (async/alts!! [command-ch result-ch (async/timeout command-poll-ms)] :priority true)]
        (cond
          (= port command-ch)
          (do
            (future-cancel job)
            {:status :stopped})

          (= port result-ch)
          (case (:status value)
            :ok {:status :ok
                 :value (:value value)}
            :error (throw (:error value)))

          :else
          (recur))))))

(defn- wait-until!! [command-ch pred]
  (loop []
    (if (pred)
      {:status :ok}
      (let [[_ port] (async/alts!! [command-ch (async/timeout command-poll-ms)] :priority true)]
        (if (= port command-ch)
          {:status :stopped}
          (recur))))))

(defn- prepare-task [db record]
  (let [task (task/ibooks-record->task record)
        path (:path task)
        ext (-> path book/file-name book/file-extension)]
    (cond
      (migrator-db/task-done? db task) nil
      (not (book/book-exists? path)) nil
      (= ext "ibooks") nil
      (= ext "epub") (assoc task :format :epub
                                 :src-path (format "/tmp/%s" (book/file-name path)))
      (= ext "pdf") (assoc task :format :pdf
                                :src-path path)
      :else nil)))

(defn- try-reserve-space! [disk-usage size limit]
  (loop [current @disk-usage]
    (let [next-usage (+ current size)]
      (cond
        (> next-usage limit) false
        (compare-and-set! disk-usage current next-usage) true
        :else (recur @disk-usage)))))

(defn- release-space-callback [disk-usage size]
  (fn []
    (swap! disk-usage #(max 0 (- % size)))))

(defn- reserve-space!! [command-ch disk-usage size limit]
  (loop []
    (if (try-reserve-space! disk-usage size limit)
      {:status :ok}
      (let [[_ port] (async/alts!! [command-ch (async/timeout command-poll-ms)] :priority true)]
        (if (= port command-ch)
          {:status :stopped}
          (recur))))))

(defn- delete-file! [path]
  (let [file (io/file path)]
    (when (and (.exists file) (not (.delete file)))
      (throw (ex-info "Unable to delete file" {:path path})))))

(defn- process-get-books-worker! [{:keys [db command-ch output-ch]}]
  (let [books (ibooks-db/get-books db)
        books-ch (async/to-chan!! books)]
    (loop []
      (let [[value port] (async/alts!! [command-ch books-ch] :priority true)]
        (cond
          (= port command-ch) :stopped
          (nil? value) :done
          :else (let [{:keys [state]} (put-output!! command-ch output-ch value)]
                  (case state
                    :ok (recur)
                    :stop :stopped
                    :output-closed :done)))))))

(defn- process-task-preparation-worker! [{:keys [db command-ch input-ch output-ch events-ch worker-key worker-index]}]
  (loop []
    (let [{:keys [state value]} (take-input!! command-ch input-ch)]
      (case state
        :stop :stopped
        :input-closed :done
        :value (if-let [task (prepare-task db value)]
                 (let [{next-state :state} (put-output!! command-ch output-ch task)]
                   (case next-state
                     :ok (recur)
                     :stop :stopped
                     :output-closed :done))
                 (do
                   (emit-event!! events-ch {:event :task-skipped
                                            :worker worker-key
                                            :worker-index worker-index
                                            :record value})
                   (recur)))))))

(defn- process-task-throttler-worker! [{:keys [max-disk-space-usage command-ch input-ch output-ch disk-usage]}]
  (loop []
    (let [{:keys [state value]} (take-input!! command-ch input-ch)]
      (case state
        :stop :stopped
        :input-closed :done
        :value (let [size (book/fs-size (:path value))
                     {reserve-state :status} (reserve-space!! command-ch disk-usage size max-disk-space-usage)]
                 (if (= reserve-state :stopped)
                   :stopped
                   (let [task (assoc value
                                     :book-size size
                                     :throttler-callback (release-space-callback disk-usage size))
                         {next-state :state} (put-output!! command-ch output-ch task)]
                     (case next-state
                       :ok (recur)
                       :stop :stopped
                       :output-closed :done))))))))

(defn- process-downloader-worker! [{:keys [command-ch input-ch output-ch]}]
  (loop []
    (let [{:keys [state value]} (take-input!! command-ch input-ch)]
      (case state
        :stop :stopped
        :input-closed :done
        :value (let [{download-state :status} (run-with-command!! command-ch #(icloud/brctl-download (:path value)))]
                 (if (= download-state :stopped)
                   :stopped
                   (let [{wait-state :status} (wait-until!! command-ch #(not (icloud/dataless? (:path value))))]
                     (if (= wait-state :stopped)
                       :stopped
                       (let [{next-state :state} (put-output!! command-ch output-ch value)]
                         (case next-state
                           :ok (recur)
                           :stop :stopped
                           :output-closed :done))))))))))

(defn- process-epub-packer-worker! [{:keys [command-ch input-ch output-ch]}]
  (loop []
    (let [{:keys [state value]} (take-input!! command-ch input-ch)]
      (case state
        :stop :stopped
        :input-closed :done
        :value (if (= :epub (:format value))
                 (let [{result-state :status} (run-with-command!! command-ch #(epub/pack-epub! (:path value) (:src-path value)))]
                   (if (= result-state :stopped)
                     :stopped
                     (let [{next-state :state} (put-output!! command-ch output-ch value)]
                       (case next-state
                         :ok (recur)
                         :stop :stopped
                         :output-closed :done))))
                 (let [{next-state :state} (put-output!! command-ch output-ch value)]
                   (case next-state
                     :ok (recur)
                     :stop :stopped
                     :output-closed :done)))))))

(defn- upload-task! [remote-host send-book-fn task]
  (let [src-md5sum (book/md5sum (:src-path task))
        remote-path (send-book-fn task)
        remote-md5sum (book/ssh-md5sum remote-host remote-path)]
    (when-not (= src-md5sum remote-md5sum)
      (throw (ex-info "Remote file checksum mismatch"
                      {:guid (:guid task)
                       :src-path (:src-path task)
                       :remote-path remote-path
                       :src-md5sum src-md5sum
                       :remote-md5sum remote-md5sum})))
    (assoc task
           :src-md5sum src-md5sum
           :remote-path remote-path
           :remote-md5sum remote-md5sum)))

(defn- process-uploader-worker! [{:keys [command-ch input-ch output-ch remote-host send-book-fn]}]
  (loop []
    (let [{:keys [state value]} (take-input!! command-ch input-ch)]
      (case state
        :stop :stopped
        :input-closed :done
        :value (let [{result-state :status
                      task :value} (run-with-command!! command-ch #(upload-task! remote-host send-book-fn value))]
                 (if (= result-state :stopped)
                   :stopped
                   (let [{next-state :state} (put-output!! command-ch output-ch task)]
                     (case next-state
                       :ok (recur)
                       :stop :stopped
                       :output-closed :done))))))))

(defn- clean-task! [task]
  (icloud/brctl-evict (:path task))
  task)

(defn- finish-cleanup!! [command-ch output-ch task]
  (let [{result-state :status} (run-with-command!! command-ch #(clean-task! task))]
    (if (= result-state :stopped)
      :stopped
      (let [{wait-state :status} (wait-until!! command-ch #(icloud/dataless? (:path task)))]
        (if (= wait-state :stopped)
          :stopped
          (if (= :epub (:format task))
            (let [{delete-state :status} (run-with-command!! command-ch #(delete-file! (:src-path task)))]
              (if (= delete-state :stopped)
                :stopped
                (let [{next-state :state} (put-output!! command-ch output-ch task)]
                  (case next-state
                    :ok :ok
                    :stop :stopped
                    :output-closed :done))))
            (let [{next-state :state} (put-output!! command-ch output-ch task)]
              (case next-state
                :ok :ok
                :stop :stopped
                :output-closed :done))))))))

(defn- process-cleaner-worker! [{:keys [command-ch input-ch output-ch]}]
  (loop []
    (let [{:keys [state value]} (take-input!! command-ch input-ch)]
      (case state
        :stop :stopped
        :input-closed :done
        :value (let [result (try
                              (finish-cleanup!! command-ch output-ch value)
                              (finally
                                (when-let [callback (:throttler-callback value)]
                                  (callback))))]
                 (case result
                   :ok (recur)
                   :stopped :stopped
                   :done :done))))))

(defn- process-finalizer-worker! [{:keys [db command-ch input-ch]}]
  (loop []
    (let [{:keys [state value]} (take-input!! command-ch input-ch)]
      (case state
        :stop :stopped
        :input-closed :done
        :value (do
                 (migrator-db/task-done! db value)
                 (recur))))))

(defn- run-worker!
  [{:keys [handler] :as worker}]
  (case handler
    :source (process-get-books-worker! worker)
    :task-preparation (process-task-preparation-worker! worker)
    :task-throttler (process-task-throttler-worker! worker)
    :downloader (process-downloader-worker! worker)
    :epub-packer (process-epub-packer-worker! worker)
    :uploader (process-uploader-worker! worker)
    :cleaner (process-cleaner-worker! worker)
    :finalizer (process-finalizer-worker! worker)))

(defn- input-worker-key
  [worker-config]
  (let [input (:input worker-config)]
    (when (self/ref? input)
      (self/worker-key input))))

(defn- start-worker-instance!
  [{:keys [worker-key
           worker-index
           handler
           command-ch
           events-ch
           global-live-workers
           stage-live-workers
           output-ch] :as worker}]
  (async/thread
    (emit-event!! events-ch {:event :started
                             :worker worker-key
                             :worker-index worker-index})
    (let [terminal-event (try
                           (case (run-worker! worker)
                             :stopped {:event :stopped
                                       :worker worker-key
                                       :worker-index worker-index}
                             :done {:event :done
                                    :worker worker-key
                                    :worker-index worker-index})
                           (catch Throwable t
                             {:event :error
                              :worker worker-key
                              :worker-index worker-index
                              :error t}))]
      (emit-event!! events-ch terminal-event)
      (when (zero? (swap! stage-live-workers dec))
        (when output-ch
          (async/close! output-ch)))
      (when (zero? (swap! global-live-workers dec))
        (async/close! events-ch)))))

(defn- start-stage! [{:keys [pipeline-config stage-configs stages events-ch global-live-workers]} worker-key]
  (or (get @stages worker-key)
      (let [user-config (get stage-configs worker-key)
            _ (when-not user-config
                (throw (ex-info "Unknown worker in pipeline config" {:worker worker-key})))
            dependency-key (input-worker-key user-config)
            input-stage (when dependency-key
                          (start-stage! {:pipeline-config pipeline-config
                                         :stage-configs stage-configs
                                         :stages stages
                                         :events-ch events-ch
                                         :global-live-workers global-live-workers}
                                        dependency-key))
            definition (merge (get worker-definitions worker-key)
                              user-config)
            scale (or (:scale definition) 1)
            output-ch (async/chan (or (:buffer-size definition) default-buffer-size))
            command-bus (async/chan 1)
            command-mult (async/mult command-bus)
            stage-live-workers (atom scale)
            stage-runtime (assoc definition
                                 :worker-key worker-key
                                 :input-ch (:output-ch input-stage)
                                 :output-ch output-ch
                                 :command-bus command-bus
                                 :command-mult command-mult
                                 :stage-live-workers stage-live-workers
                                 :self nil)]
        (swap! global-live-workers + scale)
        (doseq [worker-index (range scale)]
          (let [command-ch (async/chan 1)
                worker (assoc stage-runtime
                              :worker-index worker-index
                              :command-ch command-ch
                              :events-ch events-ch
                              :global-live-workers global-live-workers)]
            (async/tap command-mult command-ch)
            (start-worker-instance! worker)))
        (swap! stages assoc worker-key stage-runtime)
        stage-runtime)))

(defn start-pipeline!
  [pipeline-config]
  (let [events-ch (async/chan 256)
        stages (atom {})
        global-live-workers (atom 0)]
    (doseq [worker-key (keys pipeline-config)]
      (start-stage! {:pipeline-config pipeline-config
                     :stage-configs pipeline-config
                     :stages stages
                     :events-ch events-ch
                     :global-live-workers global-live-workers}
                    worker-key))
    {:events-ch events-ch
     :stages @stages}))

(defn stop-pipeline!
  ;; who uses it?
  [{:keys [stages]}]
  (doseq [{:keys [command-bus]} (vals stages)]
    (async/put! command-bus :stop)
    (async/close! command-bus)))

(defn await-pipeline!!
  [{:keys [events-ch] :as pipeline}
   {:keys [log-fn stop-on-error?] :or {stop-on-error? true}}]
  (loop [events []]
    (if-let [event (async/<!! events-ch)]
      (do
        (when log-fn
          (log-fn event))
        (when (and stop-on-error?
                   (= :error (:event event)))
          (stop-pipeline! pipeline))
        (recur (conj events event)))
      {:events events
       :errors (filter #(= :error (:event %)) events)})))
