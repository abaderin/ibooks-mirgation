(ns ibooks-migration.pipeline.workers.get-books
  (:require [clojure.core.async :as a]
            [ibooks-migration.db.ibooks :as db-ibooks]))

(defn get-books-worker
  [{:keys [db cmd-ch evt-ch out-ch]}]
  (assert db "db is required")
  (assert cmd-ch "cmd-ch is required")
  (assert evt-ch "evt-ch is required")
  (assert out-ch "out-ch is required")
  (let [result-ch (a/chan)
        task-ch (a/chan 1)]
    (a/>!! task-ch {})
    (a/go-loop []
      (let [[v ch] (a/alts! [cmd-ch result-ch task-ch] :priority true)]
        (println v)
        (cond
          (= ch cmd-ch) (case v
                          :stop (a/>! evt-ch {:kind :stopped}))

          (= ch result-ch) (let [status (-> v :status)]
                             (case status
                               :exception (a/>! evt-ch {:kind :failed :exception (-> v :exception)})
                               :ok (do
                                     (a/go
                                       (let [result (-> v :result)]
                                         (if (nil? result)
                                           (a/close! task-ch)
                                           (do
                                             (a/>! out-ch result)
                                             (a/>! task-ch result)))))
                                     (recur))))
          (= ch task-ch) (if (nil? v)
                           (a/>! evt-ch {:kind :done})
                           (do
                             (a/io-thread
                              (let [result (try
                                             {:status :ok :result (db-ibooks/get-next-book db v)}
                                             (catch Exception e {:status :exception :exception e}))]
                                (a/>!! result-ch result)))
                             (recur))))))))
