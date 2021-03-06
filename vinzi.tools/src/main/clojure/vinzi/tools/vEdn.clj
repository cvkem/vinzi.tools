(ns vinzi.tools.vEdn
  (:use	[clojure [pprint :only [pprint pp]]]
         [clojure [stacktrace :only [print-stack-trace root-cause]]]
	   [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure.edn :as edn]
            [clojure.java [io :as io]]
            [vinzi.tools
             [vExcept :as vExcept]
             [vString :as vStr]]))

(defn checked-edn
  "Transform a value to edn and read is back via de edn-reader to check whether the edn is
   valid. When not valid show where the difference is located in the edn (string-value)."
  [value]
  (let [lpf "(checked-edn): "]
    (try
      (let [edn (pr-str value)
            readEdn (clojure.edn/read-string edn)]
        (if (= readEdn value)
          edn
          (let [diff (vStr/string-difference edn (pr-str readEdn))]
            ;; TODO: is this code reachable, or do we always get an exception?
             (error lpf "Read back edn differs from original value at position " 
                    (:position diff) " substrings:"
                    "\n\torig:     " (prn-str (:orig-subs diff))
                    "\n\tmodified: " (prn-str (:mod-subs diff))))))
      (catch Exception ex
        ;; TODO: should we report differences more detailed (depending on kind of exception?
        ;; should we parse string for #-markers, as these are likelijk offending code (requires filtering)
        (error lpf "value could not be transformed to readible edn. Does it contain functions or objects?"
               "\nEDN: "(pr-str value))))))



(defn get-log-reader-aux 
  "Return a reader that maintains a log of read strings. 
   This log-reader can be queried via (report-error-status)
   to get more detailled error-messages."
  [rdr & opts]
  (let [opts (apply hash-map opts)
        MaxChars (if-let [max-chars (:max-chars opts)] max-chars 160)
        read-log (atom {:lineSeg []
                        :lineNr  1
                        :lastRead ""})
        collect-lines (fn [{:keys [lineSeg lineNr lastRead] :as read-log}]
                       ;; collect seq of string-pieces to a line
                       ;; update the current line-nr of the last line
                       ;; and truncate the seq at maxChar characters
                       (let [lineSeg (apply str lineSeg)
                             newlines (count (re-seq #"\n" lineSeg))
                             cls (count lineSeg)
                             lastRead (if (>= cls MaxChars)
                                        (subs lineSeg (- cls MaxChars))
                                        (let [lrChars (- MaxChars cls)
                                              clr (count lastRead)]
                                          (str (if (>= clr lrChars)
                                                 (subs lastRead (- clr lrChars))
                                                 lastRead)
                                                lineSeg)))]
                         {:lineSeg []
                          :lineNr (+ lineNr newlines)
                          :lastRead lastRead}
                       ))
        append-line (fn [line]
                      (swap! read-log (fn [rl]
                                        (let [rl (assoc rl :lineSeg (conj (:lineSeg rl) line))]
                                          (if (> (count (:lineSeg rl)) 20) ;;; coollect afte n chars
                                            (collect-lines rl)
                                            rl)))))
        ;; get the most recent read part (up to MaxChars) as a string.
        ;; (keep the data in the buffers too.)
        get-lastRead #(-> (swap! read-log collect-lines)
                          (dissoc :lineSeg))
        ;; get last read and clear buffers, except for :lineNr
        ;; returns a hash-map with :lastREad and :lineNr
        get-clear-lastRead! #(let [content (swap! read-log collect-lines)]
                               (swap! read-log assoc :lastRead "")
                               (dissoc content :lineSeg))
        logReader (proxy [java.io.Reader] []
                     (close [] ;;(println "Close")
                               (.close rdr))
                     (mark [long ahead] ;;(println "mark") 
                                        (.mark rdr ahead))
                     (markSupported [] ;;(println "markSupported")
                                       (.markSupported rdr))
                     (read 
                       ([] ;;(println "Read without args called")
                           (let [ret (.read rdr)]
                             (when (>= ret 0)
                               (append-line (str (char ret))))
                             ret))
                       ([buff] ;; (println "Enter read with single par") 
                                (let [ret (.read rdr buff)]
                                  (when (>= ret 0)
                                    (let[content (if (= (class buff) java.nio.CharBuffer)
                                                   (.toString buff)
                                                   (apply str buff))]
                                      (println "   with content buff=" content)
                                      (append-line content)))
                                 ret))
                       ([buff off len] ;;(println "read with 3 pars")
                                       (let [ret (.read rdr buff off len)]
                                         (when (>= 0 ret)
                                           (append-line (apply str (drop off buff))))
                                         ret))
                       )
                     (ready [] ;; (println "ready")
                               (.ready rdr))
                     (reset [] ;;(println "reset")
                               (.reset rdr))
                     (skip [n]  ;;(println "skip")
                                (.skip rdr n))
            )
        report-error-status (fn [] 
                              ;; reports status (what is read, and what is not
                              ;; and forwards orig-reader (without mark/reset)
                              (let [ca (char-array MaxChars)
                                    ret (.read rdr ca) 
                                    tail (if (>= ret MaxChars) "..." "")
                                    ca  (if (> ret 0)
                                          (apply str ca) 
                                          "") ;; content undefined when EOF
                                    {:keys [lastRead lineNr]} (get-lastRead)
                                    head (if (< (count lastRead) MaxChars) ": " ": ...")]
                                     (str "vEdn/log-reader Error when reading line "
                                          lineNr head 
                                          lastRead
                                          (if (> ret 0)
                                            (str "\nNEXT CHARS (max. " MaxChars "): " ca tail)
                                            "EOF"))))
        ]
    {:orig-reader rdr
     :reader logReader
     :report-error-status report-error-status}))


(defn get-log-str-reader 
  "Turn the content into a string-reader and package as a log-reader."
  [content]
  {:pre [(string? content)]}
  (let [rdr (java.io.StringReader. content)]
    (get-log-reader-aux rdr)))


(defn get-log-file-reader 
  "Turn the content into file-reader and package as a log-reader.
   The parameter 'f' should be a java.io.File or a filename (string)."
  [f]
  {:pre [(or (isa? (class f) java.io.File) (string? f))]}
  (let [rdr (java.io.FileReader. f)]
    (get-log-reader-aux rdr)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Reading forms from string or from file
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; cdp version
;(defn open-pushback-file
;  ([fName] (open-pushback-file fName 1))
;  ([fName size]
;     (java.io.PushbackReader. (java.io.FileReader. fName))))


(defn open-pushback-stream 
  "Generate a push-back-reader for the provided stream (a reader)."
  [rdr]
  (java.io.PushbackReader. rdr))




(defn- read-form-aux
  "Read a form from a resource. The 'pb-reader' is a reader
   containing the content. Normally you should use read-form-str or read-form-file
   to read a (series) of forms.
   Returns an array with one or more forms (so adds an additional set of array-brackets!)"
  [pb-reader]
  (letfn [(skip-start-form 
            [f]
            ;; skip white-space and return true if not EOF
            (when (.ready f)
              (let [nextChar (.read f)]
                (when (>= nextChar 0)   ;; EOF is signaled by value -1
                  ;;(println "reading: " nextChar "('" (char nextChar) "')")
                  (if (not (#{9 10 13 32} nextChar))  ;; not in "\t\n\r " 
                    (do
                      ;;(println "unreading it")
                      (.unread f nextChar)
                      true) ;; new form (non-whitespace) detected
                      (recur f))))))
          (end-of-code? 
            [forms]
            ;; an empty set marks the end of the code
            (= (last forms) ()))
          (readFormList [f cumm]
                        ;;(println "readForm: " cumm)
                        (if (end-of-code? cumm)
                          (drop-last cumm)
                          (if (skip-start-form f)
                            (recur f (conj cumm (read f))) ;; read one form
                            cumm)))]
   (readFormList pb-reader [])))

(defn logged-read
  "Read from the log-reader and process it with the processor.
   Exceptions will be caught for reporting and rethown after 
   logging the extended message to the log-file.
   (the provided log-reader is wrapped by a pushback-reader)"
  [stream-processor log-reader]
  {:pre [(fn? stream-processor) (map? log-reader)]}
   (let [{:keys [reader report-error-status]} log-reader] 
     (try
       (-> reader
          (open-pushback-stream ) 
          (stream-processor ))
       (catch Throwable ex
         (let [status (report-error-status)]
;;           (println " exception CAUGHT: report-error-status: " status)
           (vExcept/report-rethrow status ex)))
       (finally 
         (.close reader))
       )))
 
 
(defn read-form-str "Read a form from a string."
  [s]
  (logged-read read-form-aux (get-log-str-reader s)))

(defn read-form-file 
  "Read a form from a file with name 'fName'."
  ;; currently not used
  [f]
  (logged-read read-form-aux (get-log-file-reader f)))

;; NOTE: below it is shown how the lazy file is used to prepare a lazy edn reader.


; (def lazy-open
;   (letfn [(read-line [rdr]
;                      (lazy-seq
;                        (if-let [line (.readLine rdr)]
;                          (cons line (read-line rdr))
;                          (do (.close rdr)
;                            nil))))]
;     (fn [file]
;       (println "opening file: " file)
;       (lazy-seq (read-line (clojure.java.io/reader file))))))

 (defn lazy-file-open [file]
   (letfn [(read-line [rdr]
                      (lazy-seq
                        (if-let [line (.readLine rdr)]
                          (cons line (read-line rdr))
                          (do (.close rdr)
                            nil))))
           (open-file [file]
                      (println "opening file: " file)
                      (clojure.java.io/reader file))]
     (-> file
       (open-file)
       (read-line)
       (lazy-seq))))
;; (lazy-seq (read-line (open-file file)))




(defn read-edn-file 
  "Reads a edn-data from a file 'f'. If you like to put multiple data-item in a file,
   store it in a container (list, vector, hash-map).
   Does extended error reporting via log-reader to show exactly the location where
   reading failed in case of an error."
  [f]
;;  (with-open [stream (java.io.PushbackReader. (java.io.FileReader. fName))]
;;    (edn/read stream)))
 (logged-read edn/read (get-log-file-reader f)))


(defn read-edn-string
  "Reads a edn-data from string 's'. If you like to put multiple data-item in a file,
   store it in a container (list, vector, hash-map).
   Does extended error reporting via log-reader to show exactly the location where
   reading failed in case of an error."
  [s]
  (logged-read edn/read (get-log-str-reader s)))

(def ^:dynamic debugging false)

 
(defn read-edn-lazy-file 
  "Reads a lazy sequence of forms from a file.
  NOTE:  the lazy open does not have extended error reporting yet!"
  [fName]
  (let [lpf "(read-edn-lazy-file): "
;        read-entry (if debugging
;                     (fn [strs]
;                       (println "next string: " (first strs))
;                       (let [res (edn/read-string (first strs))]
;                         (println "produces data:") (pprint res)
;                         res))
;                       edn/read-string)
        read-string-ext (fn [form]
                          (try
                            (when debugging
                              (def ^:dynamic last-edn-line form)
                              (println "Next edn-string: " form))
                            (let [form (edn/read-string form)]
                              form)
                            (catch Throwable t 
                              (let [msg (str "during (edn/read-str \"" form "\") " (.getMessage t))]
                                (println msg)
                                (error msg))
                              (throw t))))]
  (map read-string-ext (lazy-file-open fName))))


(defn write-edn-file
  "Write data to an EDN file"
  [fName data]
  ;; TODO: make use of the checked-edn convertor possible via an option.
  ;;  check that we do not get duplicated quoting
  (with-open [out (java.io.FileWriter. fName)]
    (binding [*out*  out]
      (prn data))))


(defn append-edn-file
  "Append to file by opening opening file in append mode. When a high volume needs to be written
   it is better to use an open stream."
    [fName data]
  ;; TODO: make use of the checked-edn convertor possible via an option.
  ;;  check that we do not get duplicated quoting
  (with-open [out (io/writer fName :append true)]
    (binding [*out*  out]
      (prn data))))



