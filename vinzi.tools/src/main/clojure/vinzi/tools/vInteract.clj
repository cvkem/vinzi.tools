(ns vinzi.tools.vInteract
  (:require [clojure
             [string :as str]]
           [clojure.java
             [io :as io]]
  ))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tools to generate an interactive terminal
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn interactive-reader 
  "Read a line and process it by process-line. 
   If process-line returns nil, the function returns, otherwise
   the returned function is used as the processor for the next line of input."
  [process-line]
  (let [line (read-line)]
    (when-let [pl (process-line line)]
      (recur process-line))))

(defn basic-echo
  [line]
  (when-not (#{"quit" "exit"} (str/trim line))
    (println "echo: " line)
    basic-echo))

(defn get-submitter
  "Returns two submit functions that open and close a file for
   each write (force the OS to flush to the fifo-buffer)."
  [fifoName]
  (let [submit (fn [line withNl]
                  (with-open [fifo (java.io.FileWriter. fifoName)]
                    (binding [*out* fifo]
                      (if withNl (println line) (print line)))))]
    {:send-data #(submit % false)
     :send-line #(submit % true)}))


(defn get-tracker 
  "Track the file and print lines to *out*."
  [inpFile]
  (let [shutDownSignal (atom false)
        SLEEP 1000
        tracker (future
                    (println "Open file: " inpFile)
                    (with-open [isr (java.io.FileReader. inpFile)]
                      (println "File opened")
                      (loop []
                        (if @shutDownSignal
                          (println "\nForcing shutdown")
                          (if (.ready isr)
                            (let [ch (.read isr)]
                              (if (>= ch 0)
                                (do 
                                  (print (char ch))
                                  (recur))
                                (println "\nREACHED EOF")))
                            (do
                             ; (print "X") (.flush *out*)
                              (Thread/sleep SLEEP)
                              (recur)))))))]
    #(do
       (reset! shutDownSignal true)
       (Thread/sleep SLEEP))))



(comment

(defn get-tracker 
  "Track the file and print lines to *out*."
  [inpFile]
  (let [tracker (agent true)
        inp (io/reader inpFile)]
    (send-off (fn check-line [cont]
                (if-not (and cont @tracker)
                  (println " stopping as cont="cont " and tracker= " @tracker)
                  (do
                    (binding [*in* inp]
                      (println (read-line)))
                    (Thread/sleep 10)
                    (recur cont)))))))

(defn get-submitter
  [fifoName]
  (let [fifo (java.io.FileWriter. fifoName)]
    {:close (fn [] (.close fifo))
     :send-data (fn [line]
                  (binding [*out* fifo]
                    (print line)
                    (.flush fifo)))
     :send-line (fn [line]
                  (binding [*out* fifo]
                    (println line)
                    (.flush fifo)))
     :flush (fn [] (.flush fifo))}))

   ) 
