(ns ibooks-migration.pipeline.workers.prepare-task
  (:require [ibooks-migration.book :refer [file-name file-extension book-exists?]]
            [ibooks-migration.db.migrator :as db-migrator]
            [clojure.core.async :as a]))

(defn ibooks-record->task [rec]
  (-> rec
      (assoc :path (:ZBKLIBRARYASSET/ZPATH rec))
      (dissoc :ZBKLIBRARYASSET/ZPATH)
      (assoc :guid (:ZBKLIBRARYASSET/ZASSETGUID rec))
      (dissoc :ZBKLIBRARYASSET/ZASSETGUID)))

(defn prepare-task [row db tmp-root]
  (let [enrich-by-extension (fn [task]
                              (let [path (-> task :path)
                                    extension (-> path file-name file-extension)]
                                (case extension
                                  "ibooks" (assoc task :format :ibooks)
                                  "epub" (assoc task
                                                :format :epub
                                                :src-path (format "%s/%s" tmp-root (file-name path)))
                                  "pdf" (assoc task
                                               :format :pdf
                                               :src-path path))))

        task (-> row ibooks-record->task enrich-by-extension)]
    (cond
      (db-migrator/task-done? db task) [:already-done nil]
      (not (book-exists? (:path task))) [:path-not-found nil]
      (= (-> task :format) :ibooks) [:ibooks-extension nil]
      :else [nil task])))

(defn prepare-task-worker
  [{:keys [db cmd-ch evt-ch in-ch out-ch tmp-root]}]
  (let [iter-ch (a/chan 1)]
    (a/>!! iter-ch :iter)
    (a/go-loop []
      (let [[v ch] (a/alts! [cmd-ch iter-ch] :priority true)]
        (cond
          (= ch cmd-ch) (case v
                          :stop (a/>! evt-ch {:kind :stopped}))
          (= ch iter-ch) (cond
                           (instance? Throwable v) (a/>! evt-ch {:kind :failed :exception v})
                           (nil? v) (a/>! evt-ch {:kind :done})
                           :else (do
                                   (a/io-thread
                                    (let [row (a/<!! in-ch)]
                                      (try
                                        (let [[pass? task] (prepare-task row db tmp-root)]
                                          (if pass?
                                            (a/>!! iter-ch :iter)
                                            (do
                                              (a/>!! out-ch task)
                                              (a/>!! iter-ch :iter))))
                                        (catch Exception e (a/>!! iter-ch e)))))
                                   (recur))))))))
