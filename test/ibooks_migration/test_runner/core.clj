(ns ibooks-migration.test-runner.core
  (:require [cognitect.test-runner :as tr]))

(defn -main [& args]
  (apply tr/-main args))
