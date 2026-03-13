(ns ibooks-migration.db.migrator
  (:require [migratus.core :as migratus]
            [ibooks-migration.db.core :as db]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [honey.sql :as h]))

(defn migratus-config [path]
  {:store :database
   :db (db/sqlite-db-spec path)})

(defn migrate! [path]
  (migratus/migrate (migratus-config path)))

(defn connect [path]
  (jdbc/get-connection (db/sqlite-jdbc-url path)))

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
