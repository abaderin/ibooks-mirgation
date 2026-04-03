(ns ibooks-migration.book
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]))

(defn file-name [path]
  (-> path io/file .getName))

(defn file-extension [filename]
  (let [filename-parts (s/split filename #"\.")]
    (if (> (count filename-parts) 1)
      (last filename-parts)
      nil)))

(defn valid-epub? [path]
  (let [f (io/file path)]
    (and (.isDirectory f) (.exists f))))

(defn valid-pdf? [path]
  (let [f (io/file path)]
    (and (.isFile f) (.exists f))))

(defn valid-ibooks? [path]
  (let [f (io/file path)]
    (and (.isDirectory f) (.exists f))))

(defn book-exists? [path]
  (let [ext (-> path file-name file-extension)]
    (case ext
      "epub" (valid-epub? path)
      "pdf" (valid-pdf? path)
      "ibooks" (valid-ibooks? path)
      (throw (ex-info "Invalid book extension" {:extension ext})))))

(defn fs-size [path]
  (let [f (io/file path)]
    (cond
      (.isFile f) (.length f)
      (.isDirectory f) (reduce + 0 (map (comp fs-size str) (.listFiles f)))
      :else (throw (ex-info "Path does not exist" {:path path})))))

(defn md5sum [path]
  (let [{:keys [out err exit] :as sh-result} (sh "md5sum" path)]
    (if (not= exit 0)
      (throw (ex-info err (merge {:path path} sh-result)))
      (-> out (s/split #"\s+") first parse-long))))

(defn ssh-md5sum [host path]
  (let [{:keys [out err exit] :as sh-result} (sh "ssh" host (format "md5sum '%s'" path))]
    (if (not= exit 0)
      (throw (ex-info err (merge {:path path} sh-result)))
      (-> out (s/split #"\s+") first parse-long))))

(defn rm [path]
  (let [{:keys [out err exit] :as sh-result} (sh "rm" path)]
    (when-not (= exit 0)
      (throw (ex-info err (merge {:path path} sh-result))))))
