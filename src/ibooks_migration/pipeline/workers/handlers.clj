(ns ibooks-migration.pipeline.workers.handlers
  (:require [clojure.core.async :as a]))

(defmulti handle-cmd (fn [kind _] kind))

(defmethod handle-cmd :stop [_ {:keys [evt-ch] :as state}]
  (a/>! evt-ch {:kind :stopping})
  (transit state :stopping))

