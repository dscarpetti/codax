(ns codax.backup
  (:require
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]))

(defn- to-canonical-path-string [str]
  (.getCanonicalPath (io/as-file str)))

(defn- archive-files
  "available compressors are  :none  :gzip  :bzip2  :xz"
  [compressor dir dest files]
  (shell/with-sh-dir dir
    (let [[options extension] (condp = compressor
                                :none ["-cf" ".tar"]
                                :gzip ["-czf" ".tar.gz"]
                                :bzip2 ["-cjf" ".tar.bz2"]
                                :xz ["-cJf" ".tar.xz"])
          archive-result (apply shell/sh "tar" options (str dest extension) files)
          archive-path (to-canonical-path-string (str dir "/" dest extension))]
      (if (not (zero? (:exit archive-result)))
        (assoc archive-result :failure "tar")
        (let [rm-result (apply shell/sh "rm" files)]
          (if (not (zero? (:exit rm-result)))
            (assoc rm-result :failure "rm" :archive-path archive-path)
            {:archive-path archive-path}))))))

(defn make-backup-archiver
  "This returns a function for use as a :backup-fn function to an open database. It takes the
  generated files and bundles them into a tarball, optionally compressed, named
  \"backup_<iso8601 timestamp>_<nanoTime unique value>. It then calls the provided
  callback.

  Valid compressor values are:

  :none - create plain .tar files
  :gzip - create gzip compressed .tar.gz files
  :bzip2 - created bzip2 compressed .tar.bz2 files
  :xz - created xz compressed .tar.xz files

  NOTE: this functionality makes use of clojure.java.shell thus tar and/or the relevant compressor
  must be installed on the system or the process will fail.

  Once the backup archive is created (or fails to be created) the `post-archive-callback-fn` will
  be called with the result. If creation of the tarball succeeded an `:archive-path` key will provide
  the canonical file path to the new archive. If any part of the process fails a `:failure` key will
  be present and an `:err` key will provide details about the failure."
  [compressor post-archive-callback-fn]
  (assert (contains? #{nil :none :gzip :bzip2 :xz} compressor))
  (fn [{:keys [dir suffix file-names]}]
    (post-archive-callback-fn (archive-files compressor dir (str "backup" suffix) file-names))))
