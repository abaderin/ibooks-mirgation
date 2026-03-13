(ns ibooks-migration.core
  (:gen-class)
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

(defn -main
  [& _args]
  (require 'ibooks-migration.worker.core))
