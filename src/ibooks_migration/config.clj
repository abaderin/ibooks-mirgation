(ns ibooks-migration.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def ^:private required-worker-keys
  [:get-books
   :task-preparation
   :task-throttler
   :downloader
   :epub-packer
   :uploader
   :cleaner
   :finalizer])

(def ^:private worker-key-map
  {:get-books :worker/get-books
   :task-preparation :worker/task-preparation
   :task-throttler :worker/task-throttler
   :downloader :worker/downloader
   :epub-packer :worker/epub-packer
   :uploader :worker/uploader
   :cleaner :worker/cleaner
   :finalizer :worker/finalizer})

(defn parse-size [value]
  (let [s (-> value str str/trim str/lower-case)
        [_ n suffix] (or (re-matches #"(\d+(?:\.\d+)?)([kmg]?)" s)
                         (throw (ex-info "Invalid size, expected number optionally suffixed by k, m or g"
                                         {:value value})))
        n (Double/parseDouble n)
        multiplier (case suffix
                     "" 1
                     "k" 1024
                     "m" (* 1024 1024)
                     "g" (* 1024 1024 1024))]
    (long (* n multiplier))))

(defn- read-config-file [path]
  (try
    (with-open [reader (java.io.PushbackReader. (io/reader path))]
      (edn/read reader))
    (catch java.io.FileNotFoundException _
      (throw (ex-info "Config file does not exist"
                      {:config-path path})))
    (catch Exception e
      (throw (ex-info "Failed to read config file"
                      {:config-path path}
                      e)))))

(defn- blank-string? [value]
  (or (not (string? value))
      (str/blank? value)))

(defn- validate-string-field [errors config path message]
  (let [value (get-in config path)]
    (if (blank-string? value)
      (conj errors {:path path
                    :message message})
      errors)))

(defn- validate-worker-scale [errors workers worker-key]
  (let [path [:workers worker-key :scale]
        scale (get-in workers [worker-key :scale])]
    (cond
      (nil? scale)
      (conj errors {:path path
                    :message (format "Worker %s must define integer :scale" worker-key)})

      (not (integer? scale))
      (conj errors {:path path
                    :message (format "Worker %s scale must be an integer" worker-key)})

      (<= scale 0)
      (conj errors {:path path
                    :message (format "Worker %s scale must be positive" worker-key)})

      :else
      errors)))

(defn- validate-workers [errors config]
  (let [workers (:workers config)]
    (cond
      (not (map? workers))
      (conj errors {:path [:workers]
                    :message "Config must define :workers as a map"})

      :else
      (reduce (fn [acc worker-key]
                (if (contains? workers worker-key)
                  (validate-worker-scale acc workers worker-key)
                  (conj acc {:path [:workers worker-key]
                             :message (format "Config must define worker %s" worker-key)})))
              errors
              required-worker-keys))))

(defn- validate-destination [errors config]
  (let [destination (:destination config)
        type (:type destination)]
    (cond-> errors
      (not (map? destination))
      (conj {:path [:destination]
             :message "Config must define :destination as a map"})

      (and (map? destination) (not= :ssh type))
      (conj {:path [:destination :type]
             :message "Destination type must be :ssh"})

      (map? destination)
      (validate-string-field config [:destination :host] "Destination :host must be a non-empty string")

      (map? destination)
      (validate-string-field config [:destination :path] "Destination :path must be a non-empty string"))))

(defn- validate-max-disk-space [errors config]
  (let [value (:max-disk-space-usage config)]
    (cond
      (blank-string? value)
      (conj errors {:path [:max-disk-space-usage]
                    :message "Config must define :max-disk-space-usage as a non-empty string"})

      :else
      (try
        (parse-size value)
        errors
        (catch Exception e
          (conj errors {:path [:max-disk-space-usage]
                        :message (ex-message e)}))))))

(defn validate-config [config]
  (let [errors (cond-> []
                 (not (map? config))
                 (conj {:path []
                        :message "Config root must be an EDN map"}))]
    (if (seq errors)
      errors
      (-> errors
          (validate-string-field config [:ibooks-db-path] ":ibooks-db-path must be a non-empty string")
          (validate-string-field config [:migrator-db-path] ":migrator-db-path must be a non-empty string")
          (validate-max-disk-space config)
          (validate-workers config)
          (validate-destination config)))))

(defn- normalize-workers [workers]
  (reduce-kv (fn [acc worker-key worker-config]
               (assoc acc
                      (get worker-key-map worker-key worker-key)
                      {:scale (:scale worker-config)}))
             {}
             workers))

(defn normalize-config [config]
  {:ibooks-db-path (:ibooks-db-path config)
   :migrator-db-path (:migrator-db-path config)
   :max-disk-space-usage (parse-size (:max-disk-space-usage config))
   :workers (normalize-workers (:workers config))
   :destination (:destination config)})

(defn load-config! [path]
  (let [config (read-config-file path)
        errors (validate-config config)]
    (when (seq errors)
      (throw (ex-info "Config validation failed"
                      {:config-path path
                       :errors errors})))
    (normalize-config config)))
