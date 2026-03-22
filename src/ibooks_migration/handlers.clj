(ns ibooks-migration.handlers
  (:require [ibooks-migration.db.ibooks :as db-ibooks]
            [ibooks-migration.db.migrator :as db-migrator]
            [ibooks-migration.task :as task]
            [ibooks-migration.book :refer [file-name file-extension]]))

(defn throttle-task-handler [task {:keys [command-ch disk-usage max-disk-space-usage]}]
  (let [size (book/fs-size (:path task))]
    (runtime/reserve-space!! command-ch disk-usage size max-disk-space-usage)
    (assoc task
           :book-size          size
           :throttler-callback #(swap! disk-usage (fn [n] (max 0 (- n size)))))))

(defn download-task-handler [task {:keys [command-ch]}]
  (runtime/run-with-command!! command-ch #(icloud/brctl-download (:path task)))
  (runtime/wait-until!! command-ch #(not (icloud/dataless? (:path task))))
  task)

(defn pack-task-handler [task {:keys [command-ch]}]
  (when (= :epub (:format task))
    (runtime/run-with-command!! command-ch #(epub/pack-epub! (:path task) (:src-path task))))
  task)

(defn upload-task-handler [task {:keys [command-ch remote send-book-fn]}]
  (let [src-md5     (runtime/run-with-command!! command-ch #(book/md5sum (:src-path task)))
        remote-path (runtime/run-with-command!! command-ch #(send-book-fn task))
        remote-md5  (runtime/run-with-command!! command-ch #(book/ssh-md5sum (:host remote) remote-path))]
    (when-not (= src-md5 remote-md5)
      (throw (ex-info "Checksum mismatch after upload"
                      {:guid        (:guid task)
                       :src-path    (:src-path task)
                       :remote-path remote-path
                       :src-md5     src-md5
                       :remote-md5  remote-md5})))
    (assoc task :src-md5sum src-md5 :remote-path remote-path :remote-md5sum remote-md5)))

(defn delete-file! [path]
  (let [f (io/file path)]
    (when (and (.exists f) (not (.delete f)))
      (throw (ex-info "Unable to delete file" {:path path})))))

(defn cleanup-task-handler [task {:keys [command-ch]}]
  (try
    (runtime/run-with-command!! command-ch #(icloud/brctl-evict (:path task)))
    (runtime/wait-until!! command-ch #(icloud/dataless? (:path task)))
    (when (= :epub (:format task))
      (runtime/run-with-command!! command-ch #(delete-file! (:src-path task))))
    task
    (finally
      (when-let [cb (:throttler-callback task)]
        (cb)))))

(defn finalize-task-handler [task {:keys [db]}]
  (migrator-db/task-done! db task)
  nil)
