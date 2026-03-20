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


;;; Pipeline config for the iBooks migration use case.

(defn- deep-merge [a b]
  (if (and (map? a) (map? b))
    (merge-with deep-merge a b)
    b))
