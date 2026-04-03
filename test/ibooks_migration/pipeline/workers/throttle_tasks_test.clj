(ns ibooks-migration.pipeline.workers.throttle-tasks-test
  (:require [clojure.test :refer :all]
            [ibooks-migration.pipeline.workers.throttle-tasks :as worker]
            [clojure.set :refer [difference]]))

(deftest allowed-transitions
  (testing "all allowed transitions are implemented"
    (let [declared-states (-> worker/allowed-transitions keys set)
          handled-states (-> worker/handle-state methods keys set)]
      (is (empty? (difference declared-states handled-states)))))

  (testing "all handled transitions are declared in allowed-transitions"
    (let [declared-states (-> worker/allowed-transitions keys set)
          handled-states (-> worker/handle-state methods keys set)]
      (is (empty? (difference handled-states declared-states)))))

  (testing "allowed-transitions map is in consistent state"
    (let [states (-> worker/allowed-transitions keys set)
          target-states-in-states (fn [[_ target-states]]
                                    (every? states target-states))]
      (is (every? target-states-in-states worker/allowed-transitions)))))
