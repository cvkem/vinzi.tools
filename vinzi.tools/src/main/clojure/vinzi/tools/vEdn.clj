(ns vinzi.tools.vEdn
  (:use	[clojure [pprint :only [pprint pp]]]
         [clojure [stacktrace :only [print-stack-trace root-cause]]]
	   [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure.edn :as edn]
            [clojure.java [io :as io]]))


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



(defn open-pushback-file
  ([fName] (open-pushback-file fName 1))
  ([fName size]
     (java.io.PushbackReader. (java.io.FileReader. fName))))


(defn read-edn-file 
  "Reads a single form of a file. If you like to put multiple data-item in a file, store it in a container (list, vector, hash-map).
   or use read-edn-lazy-file, which assumes the file/stream contains a sequence of objects."
  [fName]
  (with-open [stream (java.io.PushbackReader. (java.io.FileReader. fName))]
    (edn/read stream)))

(def debugging true)

(defn read-edn-lazy-file 
  "Reads a lazy sequence of forms from a file."
  [fName]
  (let [read-entry (if debugging
                     (fn [strs]
                       (println "next string: " (first strs))
                       (let [res (edn/read-string (first strs))]
                         (println "produces data:") (pprint res)
                         res))
                       edn/read-string)]
  (map edn/read-string (lazy-file-open fName))))


(defn write-edn-file
  [fName data]
  (with-open [out (java.io.FileWriter. fName)]
    (binding [*out*  out]
      (prn data))))


(defn append-edn-file
  "Append to file by opening opening file in append mode. When a high volume needs to be written
   it is better to use an open stream."
    [fName data]
  (with-open [out (io/writer fName :append true)]
    (binding [*out*  out]
      (prn data))))
