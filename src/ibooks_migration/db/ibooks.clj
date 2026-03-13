(ns ibooks-migration.db.ibooks
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [honey.sql :as h]
            [clojure.pprint :refer [print-table]]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [ibooks-migration.db.core :as db]))

(def default-db-path
  "/Users/anton/Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary/BKLibrary-1-091020131601.sqlite")

(defn connect [path]
  (jdbc/get-connection (db/sqlite-jdbc-url path)))

(defn get-books [db]
  (let [q (h/format {:select [:ZPATH :ZASSETGUID]
                     :from [:ZBKLIBRARYASSET]
                     :where [:and
                             [:= :ZDATASOURCEIDENTIFIER "com.apple.ibooks.datasource.ubiquity"]
                             [:is-not :ZPATH nil]
                             [:in :ZCONTENTTYPE [1 3]]]})]
    (sql/query db q)))
