(ns ibooks-migration.db.ibooks
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [honey.sql :as h]))

(defn create-datasource [path]
  (jdbc/get-datasource (format "jdbc:sqlite:%s" path)))

(defn get-books [db]
  (let [q (h/format {:select [:ZPATH :ZASSETGUID]
                     :from [:ZBKLIBRARYASSET]
                     :where [:and
                             [:= :ZDATASOURCEIDENTIFIER "com.apple.ibooks.datasource.ubiquity"]
                             [:is-not :ZPATH nil]
                             [:in :ZCONTENTTYPE [1 3]]]})]
    (sql/query db q)))

(defn get-next-book [db zcreationdate zassetguid]
  (let [q (h/format {:select [:ZPATH :ZASSETGUID :ZCREATIONDATE]
                     :from [:ZBKLIBRARYASSET]
                     :where [:and
                             [:= :ZDATASOURCEIDENTIFIER "com.apple.ibooks.datasource.ubiquity"]
                             [:is-not :ZPATH nil]
                             [:in :ZCONTENTTYPE [1 3]]
                             [:> :ZASSETGUID zassetguid]
                             [:>= :ZCREATIONDATE zcreationdate]]
                     :order-by [[:ZCREATIONDATE :asc]
                                [:ZASSETGUID :asc]]})
]
    (->> (sql/query db q)
         first)))

(defn get-next-book
  [db {:ZBKLIBRARYASSET/keys [ZASSETGUID ZCREATIONDATE]}]
  (let [where-base [:and
                    [:= :ZDATASOURCEIDENTIFIER "com.apple.ibooks.datasource.ubiquity"]
                    [:is-not :ZPATH nil]
                    [:in :ZCONTENTTYPE [1 3]]]
        where-cursor (when (and ZCREATIONDATE ZASSETGUID)
                       [:or
                        [:> :ZCREATIONDATE ZCREATIONDATE]
                        [:and
                         [:= :ZCREATIONDATE ZCREATIONDATE]
                         [:> :ZASSETGUID ZASSETGUID]]])
        q (h/format {:select [:ZPATH :ZASSETGUID :ZCREATIONDATE]
                     :from [:ZBKLIBRARYASSET]
                     :where (if where-cursor
                              [:and where-base where-cursor]
                              where-base)
                     :order-by [[:ZCREATIONDATE :asc]
                                [:ZASSETGUID :asc]]
                     :limit 1})]
    (->> (sql/query db q)
         first)))
