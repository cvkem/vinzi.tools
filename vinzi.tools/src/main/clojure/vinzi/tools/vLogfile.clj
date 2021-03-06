(ns vinzi.tools.vLogfile
  (:use [clojure.tools logging]
	clojure.pprint)
  (:require  [clojure
              [stacktrace :as st]
              [string :as str :only [lowercase replace replace-re trim]]]
             [clojure.java
              [io :as io]
              [shell :as sh]]
             [vinzi.tools 
              [vFile :as vFile]
              [vEdn :as vEdn]
              [vExcept :as vExcept]])
  (:import [java.io     File   BufferedReader FileInputStream FileOutputStream
	    BufferedInputStream]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  function for the progress.edn file
;;  The edn is currently coupled to the log-tracker and derives it's filename from that file.
;;  Log entries have shape {:data
;;                      :... }
;;

(defn set-logback-logfile 
  "Set the logfile via the Joran programming interface. 
    Only works with logback!!
    Configuration is either a filename a java.io.File or an inputstream
    (containing xml (or groovy) (or  a list of ch.qos.logback.core.joran.event.SaxEvent)."
  [configuration]
  (let [lpf "(set-logback-logfile): "]
    (try 
      ;; based on  code-sniplet from:
      ;;   http://logback.qos.ch/manual/configuration.html
      (let [context (org.slf4j.LoggerFactory/getILoggerFactory)
            jc (ch.qos.logback.classic.joran.JoranConfigurator.)]
        (.setContext jc context)
        (.reset context)
        (.doConfigure jc configuration))
      (catch ch.qos.logback.core.joran.spi.JoranException ex
        (binding [*out* *err*]
          (println lpf "Exception raised: " (.getMessage ex))))
      (catch java.lang.ClassNotFoundException ex
         (error lpf "This function assumes logback as logging-framework\n" 
           (.getMessage ex))))))

(defn derive-edn 
  "Derive an edn file that uses the same path and basename as the logName. 
   The new file-name ends with .edn. If the logName ends in .log this suffix is skipped."
  [logName]
  (str/replace logName #"^(.+)(\.log)$" "$1.edn"))


(def progressEdnFile (atom nil))



(defn unmangle
  "Given the name of a class that implements a Clojure function, returns the function's name in Clojure. 
   Note: If the true Clojure function name contains any underscores (a rare occurrence), the unmangled name will
  contain hyphens at those locations instead."
  [class-name]
  (println "classname: "  class-name)
  (println (re-find #"^(.+)\$(.+)__\d+$" class-name))
  (str/replace class-name #"^(.+)\$(.+)(__\d*){0,1}$" "$1/$2"))

(defn unmangle-function-from-throwable 
  "Get name of function at level n from the java-throwable (default is top-level n=0)"
  ([th]
   (unmangle-function-from-throwable th 0))
  ([th n]
    (let [trace (vec (.getStackTrace th))
          entry (get trace n)]
      (when entry 
        (-> entry .getClassName unmangle)))))

(defmacro current-function-name []
  "Returns a string, the name of the current Clojure function"
  `(let [th# (Throwable.) ]
     (str (unmangle-function-from-throwable th#)
         " (followed by: " (unmangle-function-from-throwable th# 1) ")")))

(defn stringify-funcs 
  "Recursively stringify all functions in the provided data for output to an edn.
   Otherwise the progress-edn can not be read as it can not generate the functions."
  [data]
  (if (map? data)
    (zipmap (keys data) (map stringify-funcs (vals data)))
    (if (sequential? data) 
      (let [d (map stringify-funcs data)]
        (if (vector? data) (vec d) d))
      ;; the actual transform function symbols to a string.
      (if (fn? data) (str "fn: " data) data))))


(defn progress-entry-func 
  "Add a progress.edn entry with 'currFunc', 'process' 'entity' and 'data' (entity and data are optional).
   The entity is used to indicate the context in which the data should be interpreted.
   (Prefered use via macro (progress-entry).
   If the data contains function symbols these will be translated to 'fn: name', otherwise the 
   .edn file is not readible. "
  ([currFunc process] ;; just a checkpoint with a tag (process) 
    (progress-entry-func currFunc process nil nil))
  ([currFunc process data]  ;; entry with :entity nil 
    (progress-entry-func currFunc process nil data))
  ([currFunc process entity data]
    (let [defaultEdn {:filename "/tmp/progress.edn"
                      :get-line-count (fn [] -1)}
          pEdn (if-let [pEdn @progressEdnFile] 
                 pEdn
                 (do
                   (warn "No progess.edn set, so writing info to: " defaultEdn)
                   defaultEdn))]
        (let [{:keys [filename get-line-count]} pEdn
              log-line-nr  (if (and get-line-count (fn? get-line-count)) (get-line-count) -123)
              ednEntry (-> {:func currFunc
                            :process process
                            :log-line-nr log-line-nr
                            :timestamp (java.util.Date.)
                            }
                         (#(if (not (nil? entity)) (conj % [:entity entity]) %))
                         (#(if (not (nil? data)) (conj % [:data (stringify-funcs data)]) %)))]
          (when (or (not (number? log-line-nr)) (< log-line-nr 1))
            (warn " line-count infom missing: " ednEntry))
        ;;(println " going to add an entry: " ednEntry)
          (vEdn/append-edn-file filename ednEntry)))))


(defmacro progress-entry 
  "Macro that captures current function name and adds an progress.edn entry.
    The entries are:
        {:func  <name of the current function is spliced in by macro>
         :process The activity that produced this entry (Use a vector to represent nested processes).
         :entity  Optional argument representing the data-object (first of args if args has multiple arguments)
         :data    data element is third parameter if it exists, otherwise second parameter."
  [process & args]
  `(let [cfn# (current-function-name)]
         (progress-entry-func cfn# ~process ~@args)))


(defn set-progress-edn 
  "Set the progress.edn file to be used (only one file can be set simultaneously)."
  [logFileName get-log-line-count]
  {:pre [(string? logFileName) (fn? get-log-line-count)]}
  (if (not @progressEdnFile)
    (let [ednDescr {:get-line-count get-log-line-count
                    :filename (derive-edn logFileName)}]
      (debug "set-progress-edn: " ednDescr)
      (swap! progressEdnFile (fn [_] ednDescr))
      (progress-entry :set-progress-edn (assoc ednDescr :get-line-count 
                                               (str "Start counts: " ((:get-line-count ednDescr))))))  ;; change function to string
    (vExcept/throw-except "The progressEdnFile is already set to: " @progressEdnFile)))


(defn unset-progress-edn 
  "Unset the current progress.edn file"
  []
  (swap! progressEdnFile (fn [_] nil)))



(comment  ;; tests/examples
(defn test-it1 []
     (progress-entry :checkpoint))

(defn test-it2 [data]
     (progress-entry "only data" data))

(defn test-it3 [entity data]
     (progress-entry "only data" entity data)))

;;;;;;;;;;;;;;;;;;;;;;;
;;   log-file-tracker
;;   (will be moved to a separate module)

;; mask to discover the level of a log-line and extract the full prefix.
(def discoverLevel #"^\d{2}:\d{2}:\d{2}\.\d{3} \[[\w-\d]*\] ([\w]*) ")



(defn log-tracker "The log-tracker tracks a log-file and prints the message from 'showLevels' to the console, where 'showlevels' is a string or a collection of stings. The 'get-counts-log' returns count info on ALL levels.). If you provide a warnKill map then this tracker will isue warning or even kill the program when no log-activity is observed for the specified period, keys are :warnMinutes, :warnMessage, :killMinutes and :killMessage.
NOTE: log-trackers also sets and unsets the edn-file."
  ([fName showLevels]
    (println "enter log-tracker-2")
    (log-tracker fName showLevels nil))
  ([fName showLevels warnKill]
    (println "enter log-tracker-3")
    (let [showLevels (if (string? showLevels) (list showLevels) showLevels)
          lpf "(log-tracker): "
          showLevels (set (map keyword showLevels))
          org-fName fName
          fName (let [rw (vFile/file-exists fName)]
                     (if (or (not rw)            ;; file does not exists or no folder access
                             (not (:write rw)))  ;; no write permission
                       (let [newName (vFile/get-filename fName)
                             msg (str lpf "Logfile: " fName "does not exists, trying " newName " in current folder")]  
                         (debug msg) 
                         ;; we could test whether the log-file is updated by the statement above
                         ;; to see whether this is the correct log-file (however, requires a sleep before testing
                         (println msg)
                         newName)  ;; try current folder 
                       fName))
          ;; logback writes to currentfolder if designated folder is not writeable.
          file (-> fName
                 (File.))
          sleepMs 100
          sleepMs_get-counts-log 300  ;;  otherwise errors might reside in file-caches0
          {:keys [warnMinutes warnMessage killMinutes killMessage]} warnKill
          warnIter (when warnMinutes (long (* (/ 60000 sleepMs) warnMinutes)))
          killIter (when killMinutes (long (* (/ 60000 sleepMs) killMinutes)))
          ;; warnCnt and killCnt are used to set the number of ticks (one tick is sleepMs) before a warning is issued
          ;; or the jvm is killed.
          warnCnt (atom 0)
          killCnt (atom 0)
          ]
      (if (not (.exists file))
        (let [msg (str "The logfile with name " (.getCanonicalPath file) " does not exists (yet).\n"
                       "add log-statement to force opening of the file "
                       "before calling the 'log-tracker' closure.")]
          (println msg)
          (error msg))   ;; TODO/NOTE: shouldn't this be a throw except?
        ;; file exists so, continue creation
        (let [fr (java.io.FileReader. fName)
              ;;_ (println " file exists")
              br (java.io.BufferedReader. fr)
              ;;	lSeq (line-seq rdr)
              lineCnts (atom {:INFO 0
                              :ERROR 0
                              ;;			:FATAL 0   ;; only log4j, will be auto-generated
                              :WARN 0
                              :DEBUG 0
                              :TRACE 0
                              })
              lastLevelPrinted (atom false)
              stopThread (atom false)
              killProgram (atom false)
              fileClosed (atom false)
              ]
          (letfn [(increment-level
                    ;;  increment the line-count of a specific level. If a level does not exist in the
                    ;; hash-map it is created.
                    ;; When the key :total-lines is passed the :total-lines count is incremented
                    [level]
                    ;; increment the line-count of keyword level
                    (swap! lineCnts (fn [lCnts]
                                      (let [cnt (level lCnts)
                                            cnt (if cnt cnt 0)
                                            res (assoc lCnts level (inc cnt))]
                                        ;; auto-generate level if it does not exist.
                                        res)))
                    )
                  (lines-available []
                                   (.ready br))
                  (activity-observed []
                                     ;; warnCnt and killCnt are used to set the number of ticks (one tick is sleepMs) before 
                                     ;; a warning is issued or the jvm is killed.
                                     ;; (log-activity triggers this function, however, extenal programs or the edn-progress tracker
                                     ;; can issue this signal too)
                        (swap! killCnt (fn [_] 0))
                        (swap! warnCnt (fn [_] 0)))
                  (next-line
                    ;; read the next line of the log-file
                    []
                    ;; a lazy file reader (the clojure.java.io.reader
                    ;; closes the file when reaching EOF, even thought
                    ;; the log-file still might be growing.
                    (if (lines-available)
                      (let [line (.readLine br)]
                        ;;			   (print "next-line found: " line)
                        (activity-observed)
                        (increment-level :total-lines)
                        line)
                      (if (not @stopThread)
                        (do  ;; wait 100 ms and do a retry
                          (when (and warnIter
                                     (> (swap! warnCnt inc) warnIter))
                            ;; Using println instead of log-message otherwise  killCnt is disturbed
                            (println "No log-activity for "
                                     warnMinutes " minutes: " warnMessage)
                            (swap! warnCnt (fn [_] 0)))
                          (when (and killIter
                                     (> (swap! killCnt inc) killIter))
                            (let [msg (str "No log-activity for " killMinutes " minutes: " killMessage " [ABORT PROGRAM]")]
                              (error "(log-tracker): " msg)
                              (println msg))
                            (swap! stopThread (fn [_] true))
                            (doseq [x (range 10)]
                              (Thread/sleep sleepMs)) ;; yield time to the logger
                            (swap! killProgram (fn [_] true)))
                          (Thread/sleep sleepMs)
                          (recur))
                        (do
                          (.close br)
                          (swap! fileClosed (fn [_] true))
                          (when @killProgram
                            (System/exit -1))
                          nil)))
                    )  ;; signal end
                  (show []
                        ;; process all available/unread lines in the log-file and update all counts.
                        ;; a counts object is returned
                        (if-let [line (next-line)]
                          (let [[prefix level] (re-find discoverLevel line)]
                            (if level
                              (let [level (keyword level)
                                    line (apply str (drop (count prefix) line))]
                                (increment-level level)
                                (let [showIt (level showLevels)]
                                  
                                  (when showIt (println (name level) "-" line))
                                  (swap! lastLevelPrinted (fn [_] showIt))))
                              (when @lastLevelPrinted
                                ;; when multi-line output is send to the log
                                ;; then follow the print-status of the last
                                ;; logline
                                (println line)))
                            (recur))
                          (println "Log-tracker READY"))
                        )
                  (get-counts []
                             
                              ;;  sleep  for a while to give logging system and OS some
                              ;;  a little time to update the log-ile (get-counts-log is often called
                              ;;   when the program is terminated with an ERROR.
                              ;;  (Check whether it should be possible to turn additional waiting off)
                              (Thread/sleep sleepMs_get-counts-log) 
                              (loop [iter 0]
                                (when (and (< iter 5)  ;; wait at most 10 times. 
                                           (not @stopThread)
                                           (lines-available))
                                  ;; give show/next-line time to process the
                                  ;; available lines.
                                  ;;				(println "waiting for line-processing to catch up. Iteration: " iter)
                                  ;; always runs for full number of iterations.
                                  ;;  (check why)
                                  (Thread/sleep sleepMs)
                                  (recur (inc iter))))
                              (let [lCnts @lineCnts
                                    _  (println "get-counts: " lCnts)
                                    total (reduce + (vals lCnts))]
                                (assoc lCnts :total-entries total))
                              )
                  (stop-log-tracker []
                                    (swap! stopThread (fn[_] true))
                                    (println "\n\n\nStopping the log-tracker!!!\n\n\n")
                                    (trace "ShutDown tracking file (wait for Thread to stop)")
                                    (loop []
                                      (Thread/sleep sleepMs)
                                      ;; give 'show' time to print the last few lines
                                      (when (not @fileClosed)
                                        (recur)))
                                    (unset-progress-edn)
                                    (get-counts)
                                    )
                  ]
                 ;; discard all line present in the log-file before starting to track.
                 (info "Now discard all lines that pre-exist in the log-file \n"
                       "TODO: reading all lines might be slow when the logfile already is huge (find better solution)")
                 (loop []
                   (if (lines-available)
                     (let [_ (.readLine br)]
                       (recur))))

                 ;; in the back-ground print selected lines to the console
                 (.start (Thread. show))
                 ;; expose the interface
                 (let [logTrackInt {:get-counts-log get-counts
                                    :stop-log-tracker  stop-log-tracker}]
                   
                   ;; introduce an edn-file
                   (set-progress-edn fName #(:total-lines (get-counts)))

                   (println "generated structure: " logTrackInt)
                   logTrackInt)
                 ))
        ))))



(defn memory-tracker 
  "fName is the name of the file that should contain the memory report
   and interval is a reporting interval in seconds.
   (do not take interval too short as file needs to be openened/flushed 
   on each iteration).
   Options: 
     :osDiv append a snapshot of /proc/meminfo in 1 out of osDiv 
         intervals (default -1, which means 1 in MAXLONG)
     :activityDescr is a tekst to be put at the begin of the mem-trace
     :append (boolean) when set to true (a value) the current file fName
        will not be deleted/truncated."
  [fName interval & opts]
  (let [memFile  "/proc/meminfo"
        opts (apply hash-map opts)
        osDiv (or (:osDiv opts) -1)
        get-os-mem-report #(->> (sh/sh "cat" memFile) 
                             (:out )) 
        stf (java.text.SimpleDateFormat. "yyyy-MM-DD HH:mm:ss.SSS")
        nextOs (atom 1)
        get-os-rep (fn [] 
                     (let [nowMs  (.format stf (java.util.Date.)) 
                           omr  (->> (get-os-mem-report)
                                     (re-find #"MemTotal:\s*(\d*)[\w\s\d:\(\)]*MemFree:\s*(\d*)[\w\s\d:i\(\)]*SwapFree:\s*(\d*)" )
                                     (rest)
			             (map #(int (/ (Long/parseLong %) 1000)) )
			  ;; prepend the time (precision ms)
                                     ((partial cons nowMs ))
                                     (apply format "#OS: Time %s MemTotal %sMb MemFree: %sMb SwapFree: %sMb\n"))
                           ofr (-> (sh/sh "free"  "-m") (:out) (str/replace "\n" "#\n") ((partial str "#")))]
                       (str omr \newline ofr)))
        baseReport (str "### Dump of " memFile "\n" (get-os-mem-report) "###\n")
        fName (vFile/filename fName)
        _ (when (and (not (:append opts))
                          (vFile/file-exists fName))
            (io/delete-file fName))
        memTrack (io/writer fName :append (:append opts))
        add-line (fn [l]
                   (let [l (if (= (last l) \newline) l (str l \newline))]
                     (.write memTrack l) 
                     (.flush memTrack)))
        sleepMs (long (* interval 1000))
        rt (Runtime/getRuntime)
        Mb (* 1024 1024)
        mem-stat (fn []
                  (let [free (long (/ (.freeMemory rt) Mb))
                        total (long (/ (.totalMemory rt) Mb))
		        used  (- total free)
                        max    (long (/ (.maxMemory rt) Mb))]
                     (str "used " used "Mb  free " free "Mb  max. " max "Mb\n")))
        stop-thread (atom false)
        mem-track (fn []
                    (add-line (mem-stat))
                    (when (= (swap! nextOs dec) 0)
                      (add-line (get-os-rep))
                      (swap! nextOs (fn [_] osDiv)))
                    (Thread/sleep sleepMs)
		    (when (not @stop-thread)
                      (recur)))
        mem-track-thread (Thread. mem-track)
        stop-memory-tracker (fn [] 
                          (println "Stop the memory tracking process")
                          ;;(.stop mem-track-thread)
			  (swap! stop-thread (fn [_] true))  ;; .stop is deprecated.
                          (println "wait for a cycle and close file: " fName)
                          (Thread/sleep 1)  ;; wait for a cycle before closing streeam
                          (.close memTrack))
        ] 
     (println "Logging memory to: " fName)
     (when-let [ad (:activityDescr opts)]
       (let [ad (str "#" (str/replace ad #"\n" "\n#"))]
         (add-line ad)))
     (add-line baseReport)
     (.start mem-track-thread)
     {:stop-memory-tracker stop-memory-tracker}
   ))


(defn test-memory-tracker []
  (let [{:keys [stop-memory-tracker]} (memory-tracker "/tmp/test-track" 1 2)]
  (println "started the memory tracker. Now wait 10s")
  (Thread/sleep 10000)
  (println "woke up")
  (stop-memory-tracker) 
  (println "Stopped tracker. Ready")))


(defn -main [& args]
  (let[{:keys [get-counts-log stop-log-tracker]}
       (log-tracker "README" '("ERROR" "INFO") {:warnMinutes 0.5 :warnMessage "Hello"
						:killMinutes 1 :killMessage "I warned you!!"})]
    (println "Started the logger")
    (doseq [x (range 240)]
	    (Thread/sleep 1000))
    (println "Survived three minutes????")))
