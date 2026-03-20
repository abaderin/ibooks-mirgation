(ns ibooks-migration.db.migrator
  (:require [migratus.core :as migratus]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [honey.sql :as h]))

(defn migratus-config [path]
  {:store :database
   :db {:dbtype "sqlite"
        :dbname path}})

(defn migrate! [path]
  (migratus/migrate (migratus-config path)))

(defn create-datasource [path]
  (jdbc/get-datasource (format "jdbc:sqlite:%s" path)))

(defn task-done! [db {:keys [guid]}]
  (let [q (h/format {:insert-into [:task]
                     :columns [:guid :done]
                     :values [[guid 1]]
                     :on-conflict [:guid]
                     :do-update-set [:done]})]
    (-> (jdbc/execute-one! db q)
        :next.jdbc/update-count)))

(defn task-done? [db {:keys [guid]}]
  (let [q (h/format {:select [:guid :done]
                     :from [:task]
                     :where [:= :guid guid]})
        r (sql/query db q)]
    (if-let [v (-> r first :task/done)]
      (case v
        0 false
        1 true)
      false)))
