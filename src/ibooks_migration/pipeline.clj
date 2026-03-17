(ns ibooks-migration.pipeline
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as s]
            [ibooks-migration.book :as book]
            [ibooks-migration.db.ibooks :as ibooks-db]
            [ibooks-migration.db.migrator :as migrator-db]
            [ibooks-migration.epub :as epub]
            [ibooks-migration.icloud :as icloud]
            [ibooks-migration.task :as task]
            [ibooks-migration.worker.runtime :as runtime]))

;;; Handlers — (value ctx) → result or nil.
;;; Throw stopped-ex (via runtime helpers) to signal stop; other exceptions signal error.

(defn- get-books-source [{:keys [db]}]
  (ibooks-db/get-books db))

(defn- prepare-task-handler [record {:keys [db events-ch worker worker-index]}]
  (let [t    (task/ibooks-record->task record)
        path (:path t)
        ext  (-> path book/file-name book/file-extension)
        skip #(do (runtime/emit!! events-ch {:event        :task-skipped
                                             :reason       %
                                             :worker       worker
                                             :worker-index worker-index})
                  nil)]
    (cond
      (migrator-db/task-done? db t)  (skip :already-done)
      (not (book/book-exists? path)) (skip :path-not-found)
      (= ext "ibooks")               nil
      (= ext "epub")                 (assoc t :format   :epub
                                              :src-path (format "/tmp/%s" (book/file-name path)))
      (= ext "pdf")                  (assoc t :format :pdf :src-path path)
      :else                          nil)))

(defn- throttle-task-handler [task {:keys [command-ch disk-usage max-disk-space-usage]}]
  (let [size (book/fs-size (:path task))]
    (runtime/reserve-space!! command-ch disk-usage size max-disk-space-usage)
    (assoc task
           :book-size          size
           :throttler-callback #(swap! disk-usage (fn [n] (max 0 (- n size)))))))

(defn- download-task-handler [task {:keys [command-ch]}]
  (runtime/run-with-command!! command-ch #(icloud/brctl-download (:path task)))
  (runtime/wait-until!! command-ch #(not (icloud/dataless? (:path task))))
  task)

(defn- pack-task-handler [task {:keys [command-ch]}]
  (when (= :epub (:format task))
    (runtime/run-with-command!! command-ch #(epub/pack-epub! (:path task) (:src-path task))))
  task)

(defn- upload-task-handler [task {:keys [command-ch remote send-book-fn]}]
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

(defn- delete-file! [path]
  (let [f (io/file path)]
    (when (and (.exists f) (not (.delete f)))
      (throw (ex-info "Unable to delete file" {:path path})))))

(defn- cleanup-task-handler [task {:keys [command-ch]}]
  (try
    (runtime/run-with-command!! command-ch #(icloud/brctl-evict (:path task)))
    (runtime/wait-until!! command-ch #(icloud/dataless? (:path task)))
    (when (= :epub (:format task))
      (runtime/run-with-command!! command-ch #(delete-file! (:src-path task))))
    task
    (finally
      (when-let [cb (:throttler-callback task)]
        (cb)))))

(defn- finalize-task-handler [task {:keys [db]}]
  (migrator-db/task-done! db task)
  nil)

;;; send-book-fn factory: sends src-path via scp, returns the remote path.

(defn send-book-fn [host remote-root]
  (fn [{:keys [src-path]}]
    (let [remote-root (s/replace remote-root #"/+$" "")
          remote-path (format "%s/%s" remote-root (book/file-name src-path))
          target      (format "%s:%s" host remote-path)
          {:keys [err exit] :as result} (sh "scp" src-path target)]
      (when (not= exit 0)
        (throw (ex-info err (merge {:src-path src-path :remote-path remote-path} result))))
      remote-path)))

;;; Pipeline config for the iBooks migration use case.

(defn- deep-merge [a b]
  (if (and (map? a) (map? b))
    (merge-with deep-merge a b)
    b))

(defn build [config ibooks-db migrator-db send-book-fn]
  (deep-merge
   {:worker/get-books        {:source-fn get-books-source
                              :db        ibooks-db}
    :worker/task-preparation {:handler-fn prepare-task-handler
                              :input      :worker/get-books
                              :db         migrator-db}
    :worker/task-throttler   {:handler-fn throttle-task-handler
                              :input      :worker/task-preparation
                              :disk-usage (atom 0)}
    :worker/downloader       {:handler-fn download-task-handler
                              :input      :worker/task-throttler}
    :worker/epub-packer      {:handler-fn pack-task-handler
                              :input      :worker/downloader}
    :worker/uploader         {:handler-fn   upload-task-handler
                              :input        :worker/epub-packer
                              :send-book-fn send-book-fn}
    :worker/cleaner          {:handler-fn cleanup-task-handler
                              :input      :worker/uploader}
    :worker/finalizer        {:handler-fn finalize-task-handler
                              :input      :worker/cleaner
                              :db         migrator-db}}
   (or (:workers config) {})))
