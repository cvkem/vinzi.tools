(ns vinzi.tools.vMap
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
   (:require [clojure.string :as str]
             [vinzi.tools
              [vDateTime :as vDate]]))

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
            (compare-fields rec1 (get-fields rec2)))
            (map-comparator 
              [rec1 rec2]
              (loop [rv1 (get-fields rec1)
                    rv2 (get-fields rec2)]
                (if (and (seq rv1) (seq rv2))
                  (let [cmp (compare (first rv1) (first rv2))]
                        (if (not= cmp 0)
                          cmp
                          (recur (rest rv1) (rest rv2))))
                  (if (or (seq rv1) (seq rv2))
                    (if (seq rv1)
                      1      ;; seq r1 is longer
                      -1)    ;;seq r2 is longer
                    0))))   ;; same size and equal
                    ]
         {:get-fields     get-fields
          :compare-fields compare-fields
          :compare-maps   compare-maps
          :map-comparator map-comparator} ))


(defn compare-map-arrays 
  "Compare two arrays of maps, using all keys of the first map of array 'ar1'. Maps should all have the same number of keys." 
  [ar1 ar2]
  (let [lpf "(compare-map-arrays): "]
   (if (not (or (seq ar1) (seq ar2)))
     true   ;; both are nil
     (if-not (= (count ar1) (count ar2))
       (do
         (trace lpf "The two arrays differ in size (return false)")
         false)
       (let [k (keys (first ar1))
             cnt (count k)
             {:keys [compare-maps]} (get-map-comparator k)
             cmpFunc (fn [[m1 m2]] 
                       (when (or (not= (count m1) cnt)
                                 (not= (count m2) cnt)
                                 (not (compare-maps m1 m2)))
                         [m1 m2]))]
             (if-let [diff (some cmpFunc (map #(vector %1 %2) ar1 ar2))]
               (do 
                 (trace lpf "First difference observed for maps: " (with-out-str (pprint diff)))
                 false)
               true))))))


(defn get-map-str-convertor 
  "Takes a map where values are the destination types and returns a function that 
  takes a map containing strings as values returns a new map with the string-values 
  converted to the types defined in type-map.
   (type-map should contains types as returned by sql (meta-data))." 
  [typeMap]
  {:pre [(map? typeMap)]}
  (letfn [(get-convertor [tp]
                         (let [tp (if (string? tp)
                                    (let [tp (-> tp (str/trim) (str/lower-case))]
                                      (if (.startsWith tp "varchar") 
                                        :string
                                        (if (.startsWith tp "timestamp")
                                          :timestamp
                                          tp)))
                                    (if (keyword? tp)
                                      tp
                                      (throw (Exception. (str "Obtained type " tp " which is not a string or keyword")))))]
                         (case tp
                           ("integer" :int) 
                               (fn [x] (Integer/parseInt (str/trim x)))
                           ("double precision" "double" "real" :real :double) 
                               (fn [x] (Double/parseDouble (str/trim x)))
                           (:string :text "string" "text" "varchar" "character varying")
                               (fn [x] x)
                           (:date "date")  
                               vDate/convert-to-date
                           (:timestamp "timestamp") 
                               vDate/convert-to-timestamp
                           (:boolean "boolean") (fn [x] (case (str/lower-case (str/trim x))
                                                          ("t" "true") true
                                                          ("f" "false") false
                                                          (throw (Exception. (str "(vMap/get-map-str-convertor): Can not map value: " x " to a boolean")))))
                           (throw (Exception. (str "Unknown type: " tp))))))]
         (let [convertors (map (fn[[k v]] (vector k (get-convertor v))) (seq typeMap))]
           ;;(pprint convertors)
           (fn [m]
             (into {} (map (fn [[k conv]] [ k (conv (get m k))]) convertors))))))


;; Usually (small) maps have sorted keys when created in one go. However, sometimes this order gets lost
;; on maps making comparison fail. 
(defn make-sorted-map 
  "Prepare a sorted-map of x where the ordering of keys is given by the lexical order of the keys."
  [x]
  (apply sorted-map-by compare (interleave (keys x) (vals x))))


(comment 
  ;;  map-compare using sorted map, however, example shows it is not neede (at least not for small vectors)
(defn map-compare [x y]
     (let [srt-map #(apply sorted-map (apply concat (seq %)))]
       (= (srt-map x) (srt-map y))))
;=> (map-compare {:a 1 :b 2} {:b 2 :a 1})
;true
;=> (= {:a 1 :b 2} {:b 2 :a 1})
;true

)  ;; end map-compare 


