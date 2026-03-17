(ns ibooks-migration.core
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [ibooks-migration.config :as config]
            [ibooks-migration.db.ibooks :as ibooks-db]
            [ibooks-migration.db.migrator :as migrator-db]
            [ibooks-migration.pipeline :as pipeline]
            [ibooks-migration.worker.core :as worker])
  (:import [java.util.concurrent Executors ThreadFactory]))

(def ^:private core-async-worker-count
  (* 2 (.availableProcessors (Runtime/getRuntime))))

(defn- daemon-thread-factory
  [workload]
  (let [counter (atom 0)
        name-format (str "ibooks-migration-" (name workload) "-%d")]
    (reify ThreadFactory
      (^Thread newThread [_ ^Runnable runnable]
        (doto ^Thread (Thread. runnable)
          (.setName (format name-format (swap! counter inc)))
          (.setDaemon true))))))

(defn core-async-executor-factory
  [workload]
  (when (#{:compute :io :mixed :core-async-dispatch} workload)
    (Executors/newFixedThreadPool core-async-worker-count
                                  (daemon-thread-factory workload))))

(defn configure-core-async-executor!
  []
  (System/setProperty "clojure.core.async.executor-factory"
                      "ibooks-migration.core/core-async-executor-factory")
  core-async-worker-count)

(configure-core-async-executor!)

(def ^:private cli-options
  [["-c" "--config PATH" "Path to EDN config file"]])

(defn- parse-cli-options [args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)
        config-path (:config options)]
    (cond
      (seq errors)
      (throw (ex-info "Failed to parse command line options"
                      {:errors errors
                       :summary summary}))

      (nil? config-path)
      (throw (ex-info "Missing required option --config"
                      {:summary summary}))

      :else
      options)))

(defn- printable-event [event]
  (cond-> (dissoc event :error)
    (:error event) (assoc :error {:message (ex-message (:error event))
                                  :data (ex-data (:error event))})))

(defn- log-event [event]
  (binding [*out* *err*]
    (prn (printable-event event))))

(defn -main
  [& args]
  (let [{:keys [config]} (parse-cli-options args)
        cfg          (config/load-config! config)
        remote       (-> cfg :workers :worker/uploader :remote)
        send-book-fn (pipeline/send-book-fn (:host remote) (:path remote))]
    (migrator-db/migrate! (:migrator-db-path cfg))
    (with-open [ibooks-db   (ibooks-db/connect (:ibooks-db-path cfg))
                migrator-db (migrator-db/connect (:migrator-db-path cfg))]
      (let [running      (->> (pipeline/build cfg ibooks-db migrator-db send-book-fn)
                             (worker/build)
                             (worker/start!))
            shutdown-hook (Thread. #(worker/stop! running))]
        (.addShutdownHook (Runtime/getRuntime) shutdown-hook)
        (try
          (let [{:keys [errors]} (worker/await!! running {:log-fn log-event})]
            (when (seq errors)
              (throw (ex-info "Migration pipeline failed"
                              {:errors (mapv printable-event errors)}))))
          (finally
            (try
              (.removeShutdownHook (Runtime/getRuntime) shutdown-hook)
              (catch IllegalStateException _))))))))
