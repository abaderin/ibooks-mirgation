(ns ibooks-migration.task)

(defn ibooks-record->task [rec]
  (-> rec
      (assoc :path (:ZBKLIBRARYASSET/ZPATH rec))
      (dissoc :ZBKLIBRARYASSET/ZPATH)
      (assoc :guid (:ZBKLIBRARYASSET/ZASSETGUID rec))
      (dissoc :ZBKLIBRARYASSET/ZASSETGUID)))
