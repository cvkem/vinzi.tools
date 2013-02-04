(ns vinzi.tools.vFile
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [string :as str]]
            [clojure.java.io :as io]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   taken from vinzi.tools.fileTools
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def FileSep (if (= java.io.File/separator "\\")
                  "\\" "/"))

(def reFileSep (re-pattern FileSep))

(defn absolute-path? "Check whether 'fName' is an absolute path (starts with 'theSep'). To detect './' relative path too use full-path."
  [fName]
  (= FileSep (str (first (str/trim fName)))))

(defn explicit-relative-path? "Check whether 'fName' is an explicite relative path (starts with './')" 
  [fName]
  (.startsWith (str/trim fName) (str "." FileSep)))

(defn full-path? "Detect fully specified paths (paths that start with theSep '/' or './'."
     [fName]
     (or (absolute-path? fName)
         (explicit-relative-path? fName)))


(defn filename 
  "Generate a filename from 'base / filename'. If filename is an absolute path then 'base' is ignored.
   contrary to (io/file parent child) this function generates strings instead of file-objects
  and it accepts children that start with a '/' (see above)."
  [base fName]
  (let [lpf "(filename): "]
    (if (= 0 (count (str/trim base)))
      (do
        (warn lpf "Base directory is empty, so return filename unmodified: "
               fName)
        fName)
      (if (full-path? fName)
        (do
          (warn lpf "fName represents an absolute path: '" fName "',\n"
                "therefore base: '" base "' is NOT added as prefix.")
          fName)
        (let [separator (if (= FileSep (str (last base))) "" FileSep)
              res (str base separator fName)]
          (trace lpf "Generated filename: " res)
          res)))))


(defn get-current-dir 
  "Get the canonical path (no trailing /)" 
  []
  (.getCanonicalPath (java.io.File. ".")))

(defn extend-path "Extend a 'path' by prefixing the current directory to relative paths.
   If the optional 'force' parameter is true than path starting with './' will be expanded too." 
  ([path]
    (if (full-path? path)
      path
      (filename (get-current-dir) path)))
  ([path force]
    (extend-path (if (and force
                          (explicit-relative-path? path))
                   (apply str (drop 2 (seq path)))
                   path))))

(defn get-path-dir 
  "Return the containing directory/folder of path. If path ends in directory then the parent directory will be returned. "
  [path]
  (let [path (extend-path path true)]
    (-> path
      (str/split reFileSep)
      (drop-last)
      ((partial str/join FileSep)))))

(defn get-filename 
  "Get the filename of a path (or a File object). "
  [path]
    (if (= (type path) java.io.File)
      (.getName path)
      (-> path
        (str/split reFileSep)
        (last))))


(defn- rw-permissions 
  "Check the permisions on an existing file object (internal function)"
  [f]
  (let [lpf "(rw-permissions): "
        rw {:read (.canRead f)
            :write (.canWrite f)}]
    (trace lpf "read-write permissions are " rw)
    rw))


(defn file-exists 
  "Checking whether a file exists and and return the read/write permissions if it exists."
  [fName]
  (let [lpf "(file-exists): "
        f  (java.io.File. fName)]   ;; File. object does not need to be closed!
    (trace lpf "Check existence of file: " fName)
    (when-let [res (and f (.exists f) (.isFile f))]
      (rw-permissions f))))

(defn dir-exists 
  "Checking whether a directory exists and return the read/write permissions if it exists."
  [fName]
  (let [lpf "(dir-exists): "
        f  (java.io.File. fName)]   ;; java.io.File. object does not need to be closed!
    (trace lpf "Check existence of directory: " fName)
    (when-let [res (and f (.exists f) (.isFile f))]
      (rw-permissions f))))



(defn ensure-dir-exists 
  "Ensure that the directory specified in 'fName' exists (including all preceding directories). 
   If fName is a directory that needs to be created you should a a terminating file-separator (/)." 
  [fName]
  {:pre [(string? fName)]}
  (debug "(ensure-dir-exists): for file " fName)
  (let [isDir? (= (last (str/trim fName)) (first FileSep))
        f  (java.io.File. fName)]
  (if (.isDirectory f)
    (.mkdirs f)
    (let [fName (.getAbsolutePath f)
          _  (trace "file-name expands as: " fName)
          dir (str/split fName reFileSep)
          dir (if isDir? dir (drop-last dir))
;                (take (dec (count dir)) dir)
          dir (str/join FileSep dir)
          d   (java.io.File. dir)]
      (.mkdirs d)))))



(defn remove-contents-dir "Remove all contents of directory 'dName'. If a directory contains sub-directories, then a recursive deletion of all files of the sub-directory needs to be applied, as the Java File object does not allow you to delecte non-empty directories.
The directory denoted by 'dName' will empty, but will not be deleted."
  [dName]
           ;;  (A more idiomatic apprach uses the file-seq function, that
           ;;   returns a sequence of files and directories. However,
           ;;   reverse sequence such that file are removed before dirs)
  (letfn [(rm-contents-dir
           [d]
           ;;  remove alle files and directories in 'd'.
           (trace "(remove-contents-dir) directory: " (.getAbsolutePath d))
           (let [fList (.listFiles d)]
             (doseq [f fList]
               ;; (trace "removing file or directory " (.getName f))
                  (when (.isDirectory f)
                    (rm-contents-dir f))
                  (.delete f)
                  )))]
    (debug "(remove-contents-dir) Recursively remove all contents of: "
           dName "(possibly relative path)")
    (let [d (java.io.File. dName)]
      (if (and (.exists d) (.isDirectory d))
        (rm-contents-dir d)
        (debug "(remove-contents-dir): " dName
               " is not a directory or does not exist. No action")))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Walking a folder-structure
;;   - walk-fs
;;   - file-only seq 
;;   ...
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn walk-fs "Perform a recursive walk over the file-system and apply action to each node (DFS). 
The 'actOnDir' flag tells whether the action should be applied to a directory before being applied to the files >0, afterward <0 or neither 0 (default-value is don't apply action to directories).
(Action is not applied to top-level directory)."
  ;; TODO:
  ;; a more idiomatic approach is based on:
  ;;  ;;   (doseq [f (reverse (file-seq (io/file d)))]
  ;;      (action f))
  ;;  (if you do not reverse then you get a BFS walk)
  ;;(defn nwalk [fName action]
  ;;(doseq [f (file-seq (io/file fName))]
  ;;  (action f)))
  ;; has the same result as:
  ;;   (walk-fs fName action 1)
  ;;  except for the fact that it first operates on the top node.
  ;;(def r1 (with-out-str (walk-fs "." #(when (.isDirectory %) (println( .getCanonicalFile %))) 1)))
  ;; Use the reverse of the line-seq to get directories last

  ([fName action] (walk-fs fName action 0))
  ([fName action actOnDir]
  (letfn [(walk-dir [d]
                (trace "(walk) directory: " (.getAbsolutePath d))
                (let [fList (.listFiles d)]
                  (doseq [f fList]
                    (trace "visiting: " f " which is "
                             (if (.isDirectory f) "directory" "file"))
                    (if (.isDirectory f)
                      (do
                        (when (pos? actOnDir) (action f))
                        (walk-dir f)
                        (when (neg? actOnDir) (action f)))
                    (action f)))))]
         (let [f (if (= (type fName) java.io.File)
                   fName (java.io.File. fName))]
           (debug "visiting: " f " which is "
                    (if (.isDirectory f) "directory"  "file"))
           (if (.isDirectory f)
             (walk-dir f)
             (action f))))))

(comment
(map #(.getName %) (.listFiles (java.io.File. ".")
   	  (reify
   	    java.io.FileFilter
   	    (accept [this f]
   	      (.isDirectory f)))))
)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; open a lazy sequence of files
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn file-only-seq "Return a sequence of file-object that correspond to
  files. Directories are filtered out of the sequence.
  ('loc should be a string or a java.io.File.)
   If filemask is given it will be applied as a filter over filenames,
   the path-mask is applied over the canonical path including the file name."
  ([loc] (file-only-seq loc nil))
  ([loc filemask] (file-only-seq loc filemask nil))
  ([loc filemask dirmask]
     (let [file (if (= (type loc) java.io.File) loc (io/file loc))
           fileOnly (->> file
                         (file-seq)
                         (filter #(.isFile %)))
           maskedFiles (if filemask
                         (let [filemask (if (= (type filemask)
                                               java.util.regex.Pattern)
                                          filemask (re-pattern filemask))]
                               (filter #(re-find filemask
                                           (.getName %)) fileOnly))
                         fileOnly)]
       (if dirmask
         (let [get-dir (fn [f]
                         (-> f
                             (.getCanonicalPath)
                             (str/split #"/")
                             (drop-last)
                             (last)))
               dirmask (if (= (type dirmask) java.util.regex.Pattern)
                         dirmask (re-pattern dirmask))]
           (filter #(re-find dirmask (get-dir %)) maskedFiles))
         maskedFiles))))


(defn line-seq-files "Open a sequence of files and concatenate the results
  as a lazy sequence."
  [fileSeq]
  (letfn [(open-file [fName]
                     (info "(line-seq-files) now opens file: " fName)
                     (io/reader fName))]
  (apply concat (map #(line-seq (open-file %)) fileSeq))))








