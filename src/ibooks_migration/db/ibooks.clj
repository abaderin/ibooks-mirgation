(ns ibooks-migration.db.ibooks
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [honey.sql :as h]))

(defn connect [path]
  (jdbc/get-connection (format "jdbc:sqlite:%s" path)))

(defn get-books [db]
  (let [q (h/format {:select [:ZPATH :ZASSETGUID]
                     :from [:ZBKLIBRARYASSET]
                     :where [:and
                             [:= :ZDATASOURCEIDENTIFIER "com.apple.ibooks.datasource.ubiquity"]
                             [:is-not :ZPATH nil]
                             [:in :ZCONTENTTYPE [1 3]]]})]
    (sql/query db q)))
