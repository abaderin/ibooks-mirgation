(ns ibooks-migration.worker.pipeline
  (:require [clojure.core.async :as async]
            [ibooks-migration.worker.runtime :as runtime]))

(def ^:private output-buffer-size 64)

(defn- worker-stages [pipeline]
  (filter (fn [[k _]] (= "worker" (namespace k))) pipeline))

;;; Build — validates refs and prepares channels. Pure map → map.

(defn- build-stage [pipeline-config worker-key]
  (if (:output-ch (get pipeline-config worker-key))
    pipeline-config
    (let [stage     (or (get pipeline-config worker-key)
                        (throw (ex-info "Unknown worker" {:worker worker-key})))
          dep-key   (when (keyword? (:input stage)) (:input stage))
          pipeline' (if dep-key (build-stage pipeline-config dep-key) pipeline-config)
          input-ch  (when dep-key (:output-ch (get pipeline' dep-key)))
          cmd-bus   (async/chan 1)
          stage     (assoc stage
                           :input-ch     input-ch
                           :output-ch    (async/chan output-buffer-size)
                           :command-bus  cmd-bus
                           :command-mult (async/mult cmd-bus))]
      (assoc pipeline' worker-key stage))))

(defn build
  ([pipeline-config]
   (build pipeline-config (keys pipeline-config)))
  ([pipeline-config [worker-key & remaining]]
   (if worker-key
     (recur (build-stage pipeline-config worker-key) remaining)
     pipeline-config)))

;;; Start — spawns worker threads. Returns enriched pipeline.

(defn- spawn-worker! [stage worker-key worker-index events-ch]
  (let [command-ch (async/chan 1)
        ctx        (assoc stage
                          :worker       worker-key
                          :worker-index worker-index
                          :command-ch   command-ch
                          :events-ch    events-ch)]
    (async/tap (:command-mult stage) command-ch)
    (async/thread
      (if-let [source-fn (:source-fn stage)]
        (runtime/run-source! source-fn ctx)
        (runtime/run-worker! (:handler-fn stage) ctx)))))

(defn start! [pipeline]
  (let [events-ch (async/chan 256)]
    (doseq [[worker-key stage] (worker-stages pipeline)
            worker-index       (range (:scale stage))]
      (spawn-worker! stage worker-key worker-index events-ch))
    (assoc pipeline ::events-ch events-ch)))

;;; Observe — supervisor loop. Blocks until all workers stop.

(defn- stop-all! [pipeline]
  (doseq [[_ stage] (worker-stages pipeline)]
    (async/put! (:command-bus stage) :stop)))

(defn stop! [pipeline]
  (stop-all! pipeline))

(defn await!! [pipeline {:keys [log-fn]}]
  (let [events-ch (::events-ch pipeline)
        total     (reduce + (map (fn [[_ s]] (:scale s)) (worker-stages pipeline)))]
    (loop [stopped-counts {}
           live           total
           errors         []
           stopping?      false]
      (if (zero? live)
        {:errors errors}
        (let [ev (async/<!! events-ch)]
          (when log-fn (log-fn ev))
          (cond
            (= :stopped (:event ev))
            (let [wk        (:worker ev)
                  new-count (inc (get stopped-counts wk 0))]
              (when (= new-count (get-in pipeline [wk :scale]))
                (async/close! (get-in pipeline [wk :output-ch])))
              (recur (assoc stopped-counts wk new-count)
                     (dec live)
                     errors
                     stopping?))

            (= :error (:event ev))
            (do (when-not stopping? (stop-all! pipeline))
                (recur stopped-counts
                       (dec live)
                       (conj errors ev)
                       true))

            :else
            (recur stopped-counts live errors stopping?)))))))
