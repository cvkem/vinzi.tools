(ns vinzi.tools.vFile
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [string :as str]]
            [clojure.java
             [io :as io]
             [shell :as sh]]
            [vinzi.tools [vExcept :as vExcept]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   taken from vinzi.tools.fileTools
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def runningWindows (= java.io.File/separator "\\"))
(def FileSep (if runningWindows
                  "\\" "/"))
;;                  "\\\\" "/"))

(def reFileSep (re-pattern (if runningWindows
                  "\\\\" FileSep)))
;;                  "\\\\" "/")FileSep))

(def Home (if runningWindows
            (get (System/getenv) "USERPROFILE")
            (get (System/getenv) "HOME")))
  
(defn absolute-path? "Check whether 'fName' is an absolute path (starts with 'theSep'). To detect './' relative path too use full-path."
  [fName]
  (= FileSep (str (first (str/trim (str fName))))))

(defn explicit-relative-path? "Check whether 'fName' is an explicite relative path (starts with './')" 
  [fName]
  (.startsWith (str/trim (str fName)) (str "." FileSep)))


(defn home-path? "Check whether 'fName' is a home path (starts with ~/ )."
  [fName]
  (.startsWith (str/trim (str fName)) (str "~" FileSep)))


(defn parent-path? "Check whether 'fName' points to a parent directory (starts with ../ )."
  [fName]
  (or (.startsWith (str/trim (str fName)) (str ".." FileSep))
      (= (str/trim (str fName)) "..")))


(defn full-path? "Detect fully specified paths (paths that start with theSep '/' or './'."
     [fName]
     (or (absolute-path? fName)
         (home-path? fName)
;;         (parent-path? fName)
         (explicit-relative-path? fName)))

(defn strip-last-folder 
  "Strip the last folder of the 'path', but do not remove the FileSep before the last folder.
   Throws an exception if it does not exist (instead of returning empty strings."
  [path]
  (let [lpf "(strip-last-folder): "
        parentPath (str/replace path (re-pattern (str "[\\w\\s]*" FileSep "{0,1}$")) "")]
    (if (seq parentPath)
      parentPath
      (vExcept/throw-except lpf "Path " path " does not have a parent-folder"))))

(defn strip-parent-prefix 
  "Strip the parent-prefix (../) from a path.
   Throws an exception if it does not exist (instead of returning empty strings."
  [path]
  (if (= path "..") ;; no trailing / needed
    ""
    (let [lpf "(strip-parent-prefix): "
          shortenedPath (str/replace path (re-pattern (str "^\\.\\." FileSep)) "")]
      (when (not (.startsWith path (str ".." FileSep)))
        (vExcept/throw-except lpf "Path " path " does not have a parent-prefix (../)"))
      shortenedPath)))

(defn strip-dquotes 
  "Trims spaces and balanced double quotes from a file-name/path."
  [fName]
  (let [fName (str/trim fName)]
    (if (and (.startsWith fName "\"")
             (.endsWith fName "\""))
      (subs fName 1 (dec (count fName)))
      fName)))

(defn filename 
  "Generate a filename from 'base / filename'. If filename is an absolute path (or home-path or './'-path) then 'base' is ignored.
   contrary to (io/file parent child) this function generates strings instead of file-objects
  and it accepts children that start with a '/' (see above).
  The '~' is expanded to an absolute path, while './' is left as is.
  (Furthermore this function has additional logging and error-reporting that is not in (io/file).
   TODO: should strip-dquotes be performed by default? (see previous function)"
  ([fName] (filename nil fName))
  ([base fName]
    (let [lpf "(filename): "
          orgfName fName
          unify-separator #(if runningWindows
                             (str/replace % #"/" "\\\\")  ;; on windows remove "/" from paths (many java-utils generate canonical paths)
                             %)
          repl-home (fn [fName]
                     (if (home-path? fName)
                       (filename Home  (str/replace fName (re-pattern (str "^\\s*~" FileSep)) ""))
                       fName))
          base (-> base (str ) 
                 (unify-separator ))
          base (if (#{"~" (str "~" FileSep)} (str/trim base))
                 Home
                 (repl-home base))
          fName (unify-separator fName)
          fName (repl-home fName)]
      (when (or (nil? fName) (not (string? fName)))
        (vExcept/throw-except lpf "invalid value for fName: " fName))
      (if (= 0 (count (str/trim base)))
        (do
          (when (= orgfName fName)
            (warn lpf "Base directory is empty and file-name did not needed changes. return unmodified: " fName))
          fName)
        (if (full-path? fName)
          fName
          (let [process-parents (fn [base fName]
                                  (if (parent-path? fName)
                                    (recur (strip-last-folder base) (strip-parent-prefix fName))
                                    [base fName]))
                ;;_  (println "base=" base "  fName="fName)
                [base fName] (process-parents base fName)
                ;;_  (println "base=" base "  fName="fName)
                separator (if (= FileSep (str (last base))) "" FileSep)
                res (str base separator fName)]
            (trace lpf "Generated filename: " res)
            res))))))
  
  
(defn get-current-dir 
  "Get the canonical path (no trailing /)" 
  []
  (.getAbsolutePath (java.io.File. ".")))  ;; .getCanonical might fail on windows (canonical uses '/')

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

(defn get-filename-base 
  "Get the filename base of a path (or a File object), so this functions strips away the path
   and all suffixes. "
  [path]
  (-> path
    (get-filename )
    (str/split #"\.")
    (first)))


(defn- rw-permissions 
  "Check the permisions on an existing file object (internal function)"
  [f]
  (let [lpf "(rw-permissions): "
        rw {:read (.canRead f)
            :write (.canWrite f)}]
    (trace lpf "read-write permissions are " rw)
    rw))


(defn file-exists 
  "Checking whether a file exists and and return the read/write permissions if it exists.  (fName can be string or java.io.File)"
  [fName]
  (let [lpf "(file-exists): "
        f  (io/file fName)]   ;; File. object does not need to be closed!
    (trace lpf "Check existence of file: " fName)
    (when-let [res (and f (.exists f) (.isFile f))]
      (rw-permissions f))))

(defn dir-exists 
  "Checking whether a directory exists and return the read/write permissions if it exists. (fName can be string or java.io.File)"
  [fName]
  (let [lpf "(dir-exists): "
        f  (io/file fName)]   ;; java.io.File. object does not need to be closed!
    (trace lpf "Check existence of directory: " fName)
    (when-let [res (and f (.exists f) (.isDirectory f))]
      (rw-permissions f))))



(defn drop-file 
  "Drop a file if it exists."
  [nme] 
  (let [f (java.io.File. nme)]
    (if (and (.exists f) (.isFile f))
      (do
        (println "REMOVE file: "nme)
        (.delete f))
      (error "Could not find file:  " nme))))

(defn copy-file-with-date 
  "copy the input to the output-file and retain dates of file (linux specific implementation)."
  [inputFile copyFile]
  (let [inputFile (filename inputFile) 
        copyFile (filename copyFile)]    ;; expand ~/  on both
    (io/copy (java.io.File. inputFile) (java.io.File. copyFile))
    (sh/sh  "touch" "-r" inputFile copyFile)))


(defn make-dir-path 
  "Make sure path is interpreted as a directory by adding a terminating slash if it does not exist."
  [path]
  (let [path (str/trim path)]
    (if (= (last path) FileSep) path (str path FileSep))))


(defn ensure-dir-exists 
  "Ensure that the directory specified in 'fName' exists (including all preceding directories). 
   If fName is a directory that needs to be created you should a a terminating file-separator (/).
    (fName can be string or java.io.File).
   TODO: Should check whether you have write-permissions on directory!!
     should check wether directory is created (as incorrect permissions on one of the parents might result in failure.
   TODO: as cleaner encoding would be to use (.mkdirs (java.io.File. x))" 
  [fName]
  {:pre [(string? fName)]}
  (debug "(ensure-dir-exists): for file " fName)
  (let [isDir? (= (last (str/trim (str fName))) (first FileSep))
        f  (io/file fName)]
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


(defn drop-folder 
  "Drop folder, by first dropping it's contents. When passing 'false as second argument the folder will not be dropped, but only its contents."
  ([folder] (drop-folder folder true))
  ([folder dropFolder]
  {:pre [(string? folder) (= (type dropFolder) java.lang.Boolean)]}
  ;; check existence first?
    (debug "recursive drop of: " folder)
  (doseq [f (-> folder
              (java.io.File. )
              (file-seq )
              (#(if dropFolder % (rest %))) ;; take folder out of the sequence if it shouldn't be dropped.
              (reverse ))]
    (.delete f))))


(defn drop-folder-contents 
  "Drop all contents of the folder, leaving the folder empty."
  [folder]
  {:pre [(string? folder)]}
  (drop-folder folder false))


;; replaced by drop-folder-contents
;(defn remove-contents-dir "Remove all contents of directory 'dName'. If a directory contains sub-directories, then a recursive deletion of all files of the sub-directory needs to be applied, as the Java File object does not allow you to delecte non-empty directories.
;The directory denoted by 'dName' will empty, but will not be deleted."
;  [dName]
;           ;;  (A more idiomatic apprach uses the file-seq function, that
;           ;;   returns a sequence of files and directories. However,
;           ;;   reverse sequence such that file are removed before dirs)
;  (letfn [(rm-contents-dir
;           [d]
;           ;;  remove alle files and directories in 'd'.
;           (trace "(remove-contents-dir) directory: " (.getAbsolutePath d))
;           (let [fList (.listFiles d)]
;             (doseq [f fList]
;               ;; (trace "removing file or directory " (.getName f))
;                  (when (.isDirectory f)
;                    (rm-contents-dir f))
;                  (.delete f)
;                  )))]
;    (debug "(remove-contents-dir) Recursively remove all contents of: "
;           dName "(possibly relative path)")
;    (let [d (io/file dName)]
;      (if (and (.exists d) (.isDirectory d))
;        (rm-contents-dir d)
;        (debug "(remove-contents-dir): " dName
;               " is not a directory or does not exist. No action")))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Walking a folder-structure
;;   - walk-fs
;;   - file-only seq 
;;   ...
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn walk-fs "Perform a recursive walk over the file-system and apply action to each node (DFS). 
The 'actOnDir' flag tells whether the action should be applied to a directory before being applied to the files >0, afterward <0 or neither 0 (default-value is don't apply action to directories).
(Action is not applied to top-level directory)."
  ;; TODO: See drop-folder
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
         (let [f (io/file fName)]
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

(defn file-only-seq "Return a recursive sequence of file-object that correspond to
  files and files in sub-folders. Directories are filtered out of the sequence.
  ('loc should be a string or a java.io.File.)
   If filemask is given it will be applied as a filter over filenames,
   the path-mask is applied over the canonical path including the file name."
  ([loc] (file-only-seq loc nil))
  ([loc filemask] (file-only-seq loc filemask nil))
  ([loc filemask dirmask]
     (let [file (io/file loc)
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
                             (.getCanonicalPath)  ;; use canonical such the / is the separator
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


(def SimpleDateFormat (java.text.SimpleDateFormat. "dd-MM-yyyy HH:mm:ss"))

(defn get-last-modified 
  "Retrieve the last modification time and return it as a string."
  [fName]
  (->> fName (java.io.File. ) (.lastModified ) (.format SimpleDateFormat )))
  




