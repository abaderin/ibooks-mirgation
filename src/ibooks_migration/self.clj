(ns ibooks-migration.self
  (:refer-clojure :exclude [ref]))

(def ^:private ref-key ::worker)

(defn ref [worker-key]
  {ref-key worker-key})

(defn ref? [value]
  (and (map? value) (contains? value ref-key)))

(defn worker-key [value]
  (get value ref-key))
