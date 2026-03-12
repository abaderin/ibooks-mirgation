(ns ibooks-migration.epub
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as s])
  (:import [java.io BufferedOutputStream File FileOutputStream]
           [java.nio.file Files Path]
           [java.util.zip CRC32 ZipEntry ZipOutputStream]))

(def ^:private excluded-file-names
  #{"iTunesMetadata.plist" "iTunesMetadata-original.plist"})

(defn- path->entry-name [^Path root ^Path path]
  (-> (.relativize root path)
      str
      (s/replace File/separator "/")))

(defn- excluded-file? [^File file]
  (contains? excluded-file-names (.getName file)))

(defn- epub-files [^File src-dir ^Path dst-path]
  (let [root-path (.toPath src-dir)
        normalized-dst-path (.normalize (.toAbsolutePath dst-path))]
    (->> (file-seq src-dir)
         (filter #(.isFile ^File %))
         (remove excluded-file?)
         (remove #(= "mimetype" (.getName ^File %)))
         (remove #(= normalized-dst-path
                     (.normalize (.toAbsolutePath (.toPath ^File %)))))
         (sort-by #(path->entry-name root-path (.toPath ^File %))))))

(defn- add-stored-entry! [^ZipOutputStream zip-out ^String entry-name bytes]
  (let [crc (CRC32.)
        size (alength ^bytes bytes)
        entry (doto (ZipEntry. entry-name)
                (.setMethod ZipEntry/STORED)
                (.setSize size)
                (.setCompressedSize size)
                (.setCrc (.getValue crc)))]
    (.update crc ^bytes bytes 0 size)
    (.setCrc entry (.getValue crc))
    (.putNextEntry zip-out entry)
    (.write zip-out ^bytes bytes)
    (.closeEntry zip-out)))

(defn- add-deflated-entry! [^ZipOutputStream zip-out ^String entry-name ^File file]
  (.putNextEntry zip-out (ZipEntry. entry-name))
  (with-open [in (io/input-stream file)]
    (io/copy in zip-out))
  (.closeEntry zip-out))

(defn pack-epub! [src-dir dst-archive]
  (let [src-file (io/file src-dir)
        dst-file (io/file dst-archive)
        dst-path (.toPath dst-file)
        mimetype-file (io/file src-file "mimetype")]
    (when-not (.isDirectory src-file)
      (throw (ex-info "src-dir is not a directory" {:src-dir src-dir})))
    (when-not (.isFile mimetype-file)
      (throw (ex-info "Missing mimetype file" {:src-dir src-dir})))
    (when-let [parent (.getParentFile dst-file)]
      (Files/createDirectories (.toPath parent) (make-array java.nio.file.attribute.FileAttribute 0)))
    (with-open [zip-out (-> dst-file
                            FileOutputStream.
                            BufferedOutputStream.
                            ZipOutputStream.)]
      ;; EPUB requires mimetype to be the first entry and stored without compression.
      (add-stored-entry! zip-out "mimetype" (Files/readAllBytes (.toPath mimetype-file)))
      (doseq [file (epub-files src-file dst-path)]
        (add-deflated-entry! zip-out
                             (path->entry-name (.toPath src-file) (.toPath ^File file))
                             file)))
    (.getAbsolutePath dst-file)))

(defn epubcheck [path]
  (-> (sh "epubcheck" path "--json" "-")
      :out
      (json/parse-string true)
      :checker
      (select-keys [:nFatal :nUsage :nWarning :nError])))

(defn epubcheck-exp [path]
  (-> (sh "epubcheck" "--mode" "exp" path "--json" "-")
      :out
      (json/parse-string true)
      :checker
      (select-keys [:nFatal :nUsage :nWarning :nError])))
