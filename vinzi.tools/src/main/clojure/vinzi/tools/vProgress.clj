(ns vinzi.tools.vProgress
  (:use	[clojure 
         [pprint :only [pprint pp]]]
        [clojure 
         [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools 
         [logging :only [error info trace debug warn]]])
  (:require [clojure 
             [string  :as str]]
            [vinzi.tools
             [vExcept :as vExcept]
             [vLogfile :as vLog]             
             [vTimer :as vTimer]]
  ))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Definition of progress- and memory tracking via log-files.
;; However, these function don't use the database (meta-data tracking)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn gen-progress-tracker
  "This function initializes the progress- and memory- tracking returns
  an interface to them. If an analysis is running it will return nil. 
   Beware too always call the (shutdown-progress-tracker), otherwise the JVM
   will not terminate (preferably put it in finally-clause).
   This progress-tracker also signals lack of log-file activity
   and warns after 5 minutes and kills the process after 60 minutes."
  [activityDescr logFile]
  {:pre [(string? activityDescr) (string? logFile)]}
  (let [lpf "(gen-progress-tracker): "
        memFile (if (re-find #"log$" logFile)
                  (str/replace logFile #"log$" "mem")
                  (str logFile ".mem"))
        {:keys [switch-phase report] :as vt} (vTimer/gen-phase-timer :init)
        {:keys [get-counts-log stop-log-tracker]}
             (vLog/log-tracker logFile '("ERROR" "INFO")
                     {:warnMinutes 5 
                      :warnMessage (str "Is there a slow database-query, " 
                                      "or a database lock?")
                      :killMinutes 60 
                      :killMessage (str "Waited too long! Going to abort "
                         "to free all resources.")})
        {:keys [stop-memory-tracker]} (vLog/memory-tracker memFile 2 
                                        :osDiv 30
                                        :activityDescr activityDescr
                                        ;; nov 2013 (append set to true)
                                        :append true) ]
    (when (not (and get-counts-log stop-log-tracker))
      ;; initialization of tracker failed
      ;; prevent memory-tracker from keeping process alive
      (when (fn? stop-memory-tracker) 
        (stop-memory-tracker)) 
      (vExcept/throw-except lpf (str "No get-counts-log or log-tracker "
        "obtained. Is logfile '" logFile 
        "' missing or not writeable? [Abort program]")))
    ;; now build the interface functions
    (let [get-num-errors (fn [] (:ERROR (get-counts-log)))
          initialErrs (get-num-errors)
          lastReportedErrs (atom {:newErrs  0
                                  :totErrs  initialErrs})
          ;; auxiliary for the atomic update
          num-new-errors-aux #(swap! lastReportedErrs (fn [v] (let [numErrs (get-num-errors)]
             ;; two when for debugging only
             (when-not (map? v) 
               (debug lpf "num-new-errors-aux: old value of type " (type v) " and value " v))
            (when-not (number? numErrs) 
              (debug lpf "num-new-errors-aux: received numErrs=" numErrs))
                                                          {:newErrs (- numErrs (:totErrs v))
                                                           :totErrs numErrs})))
          ;; return number of new-errors, or nil when there are no new errors
          num-new-errors #(let [ne (:newErrs (num-new-errors-aux))]
                            (when (> ne 0) ne))
          ;;  report total errors since start of tracker or nil. No update of newErrors!
          total-errors   #(let [te (- (get-num-errors) initialErrs)]
                            (when (> te 0) te))
          add-progress-log (fn [phase msg]
              ;; Adds a progress message to the log-file
              ;; Return the (enriched message)
              ;; first signal a switch of phases
              (switch-phase phase)
              ;; TO DO: add support for json fields when needed
              (let [lpf "(add-progress-log): "
                    ;; add the error-count only when present
                    numErr (:ERROR (get-counts-log))
                    msg    (if (> numErr 0) 
                             (str msg " (FOUTEN " numErr ")")
                             msg)]
                    (info "Add-progress-meta: " msg)
                    msg))
          shutdown-progress-tracker (fn [finalMessage]
            ;;Clean-up log-trackers and finalize metaData and progress data.
            (switch-phase :finish)
            (add-progress-log :finish finalMessage)
            (info (str "-------------------------------------------------------\n")
                       (if-let [te (total-errors)]
                         (str "In total " te " ERRORS detected during process:" activityDescr 
                           ".\n Inspect " logFile " for detailled information." )
                         (str "The process:" activityDescr "reported no errors"))
                       "-------------------------------------------------------\n")
            (stop-memory-tracker)
            (let [logStats (stop-log-tracker)]
              (println "\nThe log-file " logFile " contains:")
              (pprint logStats)))]
        {:shutdown-progress-tracker shutdown-progress-tracker
         :add-progress-log          add-progress-log
         :num-new-errors            num-new-errors
         :total-errors              total-errors
         :get-counts-log            get-counts-log 
         :get-num-errors            get-num-errors})))  


