(ns ibooks-migration.worker.core
  (:require [ibooks-migration.worker.pipeline :as pipeline]))

(def build   pipeline/build)
(def start!  pipeline/start!)
(def stop!   pipeline/stop!)
(def await!! pipeline/await!!)
