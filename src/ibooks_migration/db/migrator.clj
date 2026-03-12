(ns ibooks-migration.db.migrator
  (:require [migratus.core :as migratus]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [honey.sql :as h]))

(def config {:store :database
             :db {:dbtype "sqlite"
                  :dbname "db.sqlite"}})

(def db-dsn (format "jdbc:sqlite:%s" "db.sqlite"))
(def db-conn (jdbc/get-connection db-dsn))

(migratus/migrate config)

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
