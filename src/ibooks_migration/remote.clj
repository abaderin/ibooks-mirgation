(ns ibooks-migration.remote
  (:require [clojure.java.shell :refer [sh]]))

(defn send-file [host remote-root src-path]
  (let [scp-dst (format "%s:%s" host remote-root)
        {:keys [exit err] :as scp-result} (sh "scp" src-path scp-dst)]
    (when-not (= exit 0)
      (throw (ex-info err
                      (merge scp-result
                             {:host host
                              :remote-root remote-root
                              :src-path src-path}))))))
