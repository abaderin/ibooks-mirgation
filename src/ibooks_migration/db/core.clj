(ns ibooks-migration.db.core
  (:require [next.jdbc :as jdbc]))

(defn sqlite-db-spec [path]
  {:dbtype "sqlite"
   :dbname path})

(defn sqlite-jdbc-url [path]
  (format "jdbc:sqlite:%s" path))

(defn connect-sqlite [path]
  (jdbc/get-connection (sqlite-jdbc-url path)))
