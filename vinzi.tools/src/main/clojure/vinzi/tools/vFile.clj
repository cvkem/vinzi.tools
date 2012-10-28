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








