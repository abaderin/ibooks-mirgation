(ns ibooks-migration.worker.runtime
  (:require [clojure.core.async :as async]))

(def ^:private poll-ms 100)

(defn stopped-ex []
  (ex-info "Worker stopped" {::stopped true}))

(defn stopped? [t]
  (boolean (::stopped (ex-data t))))

(defn emit!! [events-ch event]
  (when events-ch
    (async/>!! events-ch (assoc event :timestamp (System/currentTimeMillis)))))

;; Runs thunk in a future so blocking I/O doesn't stall command-ch polling.
;; Returns thunk result; throws stopped-ex on command, re-throws thunk exceptions.
(defn run-with-command!! [command-ch thunk]
  (let [result-ch (async/promise-chan)
        job (future
              (try
                (async/>!! result-ch {:ok true :value (thunk)})
                (catch Throwable t
                  (async/>!! result-ch {:ok false :error t}))))]
    (loop []
      (let [[v port] (async/alts!! [command-ch result-ch (async/timeout poll-ms)] :priority true)]
        (cond
          (= port command-ch) (do (future-cancel job) (throw (stopped-ex)))
          (= port result-ch)  (if (:ok v) (:value v) (throw (:error v)))
          :else               (recur))))))

;; Polls pred every poll-ms. Throws stopped-ex if command arrives.
(defn wait-until!! [command-ch pred]
  (loop []
    (if (pred)
      nil
      (let [[_ port] (async/alts!! [command-ch (async/timeout poll-ms)] :priority true)]
        (if (= port command-ch)
          (throw (stopped-ex))
          (recur))))))

;; CAS loop: atomically claims size bytes from disk-usage atom; retries on concurrent modification.
(defn- try-reserve! [disk-usage size limit]
  (loop [current @disk-usage]
    (let [next (+ current size)]
      (cond
        (> next limit)                        false
        (compare-and-set! disk-usage current next) true
        :else                                 (recur @disk-usage)))))

;; Blocks until size bytes can be reserved or command arrives. Throws stopped-ex on command.
(defn reserve-space!! [command-ch disk-usage size limit]
  (loop []
    (if (try-reserve! disk-usage size limit)
      nil
      (let [[_ port] (async/alts!! [command-ch (async/timeout poll-ms)] :priority true)]
        (if (= port command-ch)
          (throw (stopped-ex))
          (recur))))))

(defn- put-output!! [command-ch output-ch value]
  (when output-ch
    (let [[_ port] (async/alts!! [command-ch [output-ch value]] :priority true)]
      (when (= port command-ch)
        (throw (stopped-ex))))))

(defn- terminal-event [ctx t]
  (emit!! (:events-ch ctx)
          (cond-> {:event        (if (or (nil? t) (stopped? t)) :stopped :error)
                   :worker       (:worker ctx)
                   :worker-index (:worker-index ctx)}
            (and t (not (stopped? t))) (assoc :error t))))

;; Source wrapper: source-fn returns a seq; feeds items to output-ch while watching command-ch.
(defn run-source! [source-fn {:keys [output-ch command-ch] :as ctx}]
  (emit!! (:events-ch ctx) {:event :started :worker (:worker ctx) :worker-index (:worker-index ctx)})
  (try
    (let [items-ch (async/to-chan!! (source-fn ctx))]
      (loop []
        (let [[value port] (async/alts!! [command-ch items-ch] :priority true)]
          (cond
            (= port command-ch) nil
            (nil? value)        nil
            :else               (do (put-output!! command-ch output-ch value)
                                    (recur))))))
    (terminal-event ctx nil)
    (catch Throwable t
      (terminal-event ctx t))))

;; Worker wrapper: reads from input-ch, calls handler, writes non-nil results to output-ch.
;; handler: (value ctx) → result or nil. May throw stopped-ex; other throws signal error.
(defn run-worker! [handler {:keys [input-ch command-ch] :as ctx}]
  (emit!! (:events-ch ctx) {:event :started :worker (:worker ctx) :worker-index (:worker-index ctx)})
  (try
    (loop []
      (let [[value port] (async/alts!! [command-ch input-ch] :priority true)]
        (cond
          (= port command-ch) nil
          (nil? value)        nil
          :else               (do (when-let [result (handler value ctx)]
                                    (put-output!! command-ch (:output-ch ctx) result))
                                  (recur)))))
    (terminal-event ctx nil)
    (catch Throwable t
      (terminal-event ctx t))))
