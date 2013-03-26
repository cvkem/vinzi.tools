(ns vinzi.tools.vBase
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
   (:require [clojure.string :as str]))



(defn indexed 
  "Generate indexed pairs of sequence sq, use-case:  (doseq [i (indexed sq)] (println i)) "
  [sq]
  {:pre [(sequential? sq)]}
  (map #(vector %1 %2) (next (range)) sq))