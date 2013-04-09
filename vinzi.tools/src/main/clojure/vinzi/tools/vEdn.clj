(ns vinzi.tools.vEdn
  (:use	[clojure [pprint :only [pprint pp]]]
         [clojure [stacktrace :only [print-stack-trace root-cause]]]
	   [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure.edn :as edn]))





(defn open-pushback-file
  ([fName] (open-pushback-file fName 1))
  ([fName size]
     (java.io.PushbackReader. (java.io.FileReader. fName))))


(defn read-edn-file 
  "Reads a single form of a file. If you like to put multiple data-item in a file, store it in a container (list, vector, hash-map)."
  [fName]
  (with-open [stream (java.io.PushbackReader. (java.io.FileReader. fName))]
    (edn/read stream)))