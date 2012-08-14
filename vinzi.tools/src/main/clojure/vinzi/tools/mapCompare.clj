(ns vinzi.tools.mapCompare
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]]))

(defn get-map-comparator 
  "Returns a comparator that allows maps to be compared on a predefined set of keys, to extract value-seqs from the map, and to compare mmaps to a values. The relevant keys are determined by cmpFlds." 
  [cmpFlds]
  (letfn [(get-fields
            ;; get all specified fields as a (lazy) sequence of values.
            [rec]
            (map rec cmpFlds))
          (compare-fields
            ;; compare map 'rec' against the sequence of values (returns bool)
            [rec values]
            (when-let [recVals (get-fields rec)]
              ;; august 12, 2012: added fix to check length of lists are the same.
              ;; bug previously not detected as compare-fields was only used indirectly. 
              (when (= (count recVals) (count values))  ;; number of fields should match (otherwise nil)
                  (let [cmp (map #(= %1 %2) recVals values)
                        someFalse (some not cmp) ]
                    (not someFalse)))))
            (compare-maps
            ;; compare two maps on their 'cmpFlds' only (returns boolean)
            [rec1 rec2]
            (compare-fields rec1 (get-fields rec2)))]
         {:get-fields     get-fields
          :compare-fields compare-fields
          :compare-maps   compare-maps} ))


