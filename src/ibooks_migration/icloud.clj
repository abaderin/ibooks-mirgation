(ns ibooks-migration.icloud
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as s]))

(defn dataless? [path]
  (let [{:keys [out err exit] :as sh-result} (sh "stat" "-f" "%Sf" path)]
    (when-not (= exit 0)
      (throw (ex-info err (merge {:path path} sh-result))))
    (s/includes? out "dataless")))

(defn brctl-download [path]
  (let [{:keys [err exit] :as sh-result} (sh "brctl" "download" path)]
    (when-not (= exit 0)
      (throw (ex-info err (merge {:path path} sh-result))))))

(defn brctl-evict [path]
  (let [{:keys [err exit] :as sh-result} (sh "brctl" "evict" path)]
    (when-not (= exit 0)
      (throw (ex-info err (merge {:path path} sh-result))))))
