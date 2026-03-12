(ns ibooks-migration.playground
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [honey.sql :as h]
            [clojure.pprint :refer [print-table pprint]]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [ibooks-migration.db.ibooks :as db-ibooks]
            [ibooks-migration.epub :refer [epubcheck pack-epub! epubcheck-exp]]
            [ibooks-migration.book :refer [file-name]])
  (:import [java.util UUID]))

(defn parse-uuid [u]
  (try
    (UUID/fromString u)
    (catch IllegalArgumentException _ nil)))

(def db-path "/Users/anton/Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary/BKLibrary-1-091020131601.sqlite")
(def db-dsn (format "jdbc:sqlite:%s" db-path))

(def db-conn (jdbc/get-connection db-dsn))

(comment "all epub files being packed by pack-epub! correctly"
         (->> (db-ibooks/get-books db-conn)
              (map :ZBKLIBRARYASSET/ZPATH)
              (filter #(s/ends-with? % ".epub"))
              (pmap (fn [path]
                      (let [tmp-path (format "/tmp/%s" (file-name path))]
                        (pack-epub! path tmp-path)
                        {:unpacked (epubcheck-exp path) :packed (epubcheck tmp-path)})))
              (every? (fn [{:keys [packed unpacked]}]
                        (= packed unpacked)))))
;; => true

