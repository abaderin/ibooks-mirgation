(ns ibooks-migration.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [malli.core :as m]
            [malli.error :as me]))

(defn parse-size [value]
  (let [s (-> value str str/trim str/lower-case)
        [_ n suffix] (or (re-matches #"(\d+(?:\.\d+)?)([kmg]?)" s)
                         (throw (ex-info "Invalid size, expected number optionally suffixed by k, m or g"
                                         {:value value})))
        multiplier (case suffix
                     "" 1
                     "k" 1024
                     "m" (* 1024 1024)
                     "g" (* 1024 1024 1024))]
    (long (* (Double/parseDouble n) multiplier))))

(defn- decode-size [value]
  (try (parse-size value) (catch Exception _ value)))

(defn- read-config-file [path]
  (try
    (with-open [reader (java.io.PushbackReader. (io/reader path))]
      (edn/read reader))
    (catch java.io.FileNotFoundException _
      (throw (ex-info "Config file does not exist" {:config-path path})))
    (catch Exception e
      (throw (ex-info "Failed to read config file" {:config-path path} e)))))

(defn- worker-schema
  ([worker-key] (worker-schema worker-key []))
  ([worker-key extra-keys]
   (into [:map {:closed true :error/message (format "Worker %s must be a map" worker-key)}
          [:scale [:int {:min 1 :error/message (format "Worker %s scale must be a positive integer" worker-key)}]]]
         extra-keys)))

(def ^:private config-schema
  (m/schema
   [:map {:closed true}
    [:ibooks-db-path   [:string {:min 1 :error/message ":ibooks-db-path must be a non-empty string"}]]
    [:migrator-db-path [:string {:min 1 :error/message ":migrator-db-path must be a non-empty string"}]]
    [:workers
     [:map {:closed true :error/message "Config must define :workers as a map"}
      [:worker/get-books        (worker-schema :worker/get-books)]
      [:worker/task-preparation (worker-schema :worker/task-preparation)]
      [:worker/task-throttler   (worker-schema :worker/task-throttler
                                               [[:max-disk-space-usage [:int {:min 1}]]])]
      [:worker/downloader       (worker-schema :worker/downloader)]
      [:worker/epub-packer      (worker-schema :worker/epub-packer)]
      [:worker/uploader         (worker-schema :worker/uploader
                                               [[:remote
                                                 [:map {:closed true}
                                                  [:type [:= {:error/message "Remote type must be :ssh"} :ssh]]
                                                  [:host [:string {:min 1}]]
                                                  [:path [:string {:min 1}]]]]])]
      [:worker/cleaner          (worker-schema :worker/cleaner)]
      [:worker/finalizer        (worker-schema :worker/finalizer)]]]]))

(def ^:private humanize-options
  {:errors
   {::m/missing-key
    {:error/fn {:en (fn [{:keys [path]} _]
                      (str "missing required key " (pr-str (peek path))))}}}})

(defn- flatten-humanized
  ([humanized]
   (flatten-humanized [] humanized))
  ([path humanized]
   (cond
     (map? humanized)
     (mapcat (fn [[k v]] (flatten-humanized (conj path k) v)) humanized)

     (vector? humanized)
     [{:path path :message (str/join "; " humanized)}]

     :else [])))

(defn- explain->errors [explanation]
  (-> explanation (me/humanize humanize-options) flatten-humanized vec))

(defn- namespace-worker-keys [config]
  (update config :workers update-keys #(keyword "worker" (name %))))

(defn load-config! [path]
  (let [config (-> path
                   read-config-file
                   (update-in [:workers :task-throttler :max-disk-space-usage] decode-size)
                   namespace-worker-keys)]
    (when-let [explanation (m/explain config-schema config)]
      (throw (ex-info "Config validation failed"
                      {:config-path path
                       :errors      (explain->errors explanation)})))
    config))
