(ns ibooks-migration.pipeline.workers.core
  (:require [clojure.set :refer [difference]]
            [clojure.core.async :as a]))

(defn transit [allowed-transitions [{:keys [state] :as s}] to-state]
  (when-not (keyword? (-> allowed-transitions state to-state))
    (throw (ex-info "Illegal transit" {:state-from state
                                       :state-to to-state})))
  (assoc s :state to-state))

(defn terminal-states [allowed-transitions]
  (->> allowed-transitions
       (keep (fn [[state next-states]]
               (when (empty? next-states)
                 state)))
       set))

(defn terminal-state? [allowed-transitions state]
  (let [terminal-states (memoize terminal-states)
        states (terminal-states allowed-transitions)]
    (contains? states state)))

(defn run-in-io-thread [nil-val f & args]
  (let [result-ch (a/chan 1)]
    (a/io-thread
     (let [result (try (apply f args) (catch Throwable e e))
           result-val (or result nil-val)]
       (a/>!! result-ch result-val)
       (a/close! result-ch)))
    result-ch))
