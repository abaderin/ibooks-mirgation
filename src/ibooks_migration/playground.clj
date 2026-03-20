(ns ibooks-migration.playground
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [honey.sql :as h]
            [clojure.pprint :refer [print-table pprint]]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.core.async :as a]
            [ibooks-migration.db.ibooks :as db-ibooks]
            [ibooks-migration.epub :refer [epubcheck pack-epub! epubcheck-exp]]
            [ibooks-migration.book :refer [file-name]]
            [ibooks-migration.pipeline.workers.get-books :as get-books-worker])
  (:import [java.util UUID]))

(defn parse-uuid [u]
  (try
    (UUID/fromString u)
    (catch IllegalArgumentException _ nil)))

(comment "all epub files being packed by pack-epub! correctly"
         (->> (db-ibooks/get-books db-conn)
              (map :ZBKLIBRARYASSET/ZPATH)
              (filter #(s/ends-with? % ".epub"))
              (pmap (fn [path]
                      (let [tmp-path (format "/tmp/%s" (file-name path))]
                        (pack-epub! path tmp-path)
                        {:unpacked (epubcheck-exp path) :packed (epubcheck tmp-path)})))
              (every? (fn [{:keys [packed unpacked]}]
                        (= packed unpacked))))
         )
;; => true

(comment
  (def db-path "/Users/anton/Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary/BKLibrary-1-091020131601.sqlite")
  )


(comment "count all rows iteratively"
         (let [ds (db-ibooks/create-datasource "/Users/anton/Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary/BKLibrary-1-091020131601.sqlite")]
           (loop [pointer {}
                  count 0]
             (when (and (pos? count) (zero? (mod count 100)))
               (println (format "count = %d" count)))
             (if-let [row (db-ibooks/get-next-book ds pointer)]
               (recur row (inc count))
               (do
                 (println (format "count = %d" count))
                 (println "done")))))
         )

(let [ds (db-ibooks/create-datasource "/Users/anton/Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary/BKLibrary-1-091020131601.sqlite")
      cmd-ch (a/chan)
      evt-ch (a/chan)
      out-ch (a/chan)]

  (a/go-loop []
    (when-some [_ (a/<! out-ch)]
      (recur)))

  (get-books-worker/get-books-worker {:db ds :cmd-ch cmd-ch :evt-ch evt-ch :out-ch out-ch})
  (println (a/<!! evt-ch)))
