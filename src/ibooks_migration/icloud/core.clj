(ns ibooks-migration.icloud.core
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as s]))

(defn dataless? [path]
  (let [{:keys [out err exit] :as sh-result} (sh "stat" "-f" "%Sf" path)]
    (if (not= exit 0)
      (throw (ex-info err (merge {:path path} sh-result))))
    (s/includes? out "dataless")))

(defn brctl-download [path]
  (let [{:keys [out err exit] :as sh-result} (sh "brctl" "download" path)]
    (if (not= exit 0)
      (throw (ex-info err (merge {:path path} sh-result))))))

(defn brctl-evict [path]
  (let [{:keys [out err exit] :as sh-result} (sh "brctl" "evict" path)]
    (if (not= exit 0)
      (throw (ex-info err (merge {:path path} sh-result))))))
