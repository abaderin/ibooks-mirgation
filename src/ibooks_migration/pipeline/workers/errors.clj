(ns ibooks-migration.pipeline.workers.errors
  (:require [clojure.core.async :as a]))

(defn set-out-ch-closed-error
  ([s] (set-out-ch-closed-error s {}))
  ([{:keys [state worker] :as s} opts]
   (-> s
       (assoc-in [:error :state] state)
       (assoc-in [:error :action] :sending-task-to-out-ch)
       (assoc-in [:error :worker] worker)
       (update-in [:error] (partial merge opts)))))

(defn set-in-ch-closed-error
  ([s] (set-in-ch-closed-error s {}))
  ([{:keys [state worker] :as s} opts]
   (-> s
       (assoc-in [:error :state] state)
       (assoc-in [:error :action] :in-ch-pulling)
       (assoc-in [:error :worker] worker)
       (update-in [:error] (partial merge opts)))))

(defn set-evt-ch-closed-error
  ([s] (set-evt-ch-closed-error s {}))
  ([{:keys [state worker] :as s} opts]
   (-> s
       (assoc-in [:error :state] state)
       (assoc-in [:error :action] :sending-to-evt-ch)
       (assoc-in [:error :worker] worker)
       (update-in [:error] (partial merge opts)))))
