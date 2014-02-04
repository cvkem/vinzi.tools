(ns vinzi.tools.vMap
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
   (:require [clojure.string :as str]
             [vinzi.tools
              [vExcept :as vExcept]
              [vDateTime :as vDate]]))

(defn get-map-comparator 
  "Returns a comparator that allows maps to be compared on a predefined set of keys. The returned object contains:
      - (get-fields [rec]): to extract value-seqs from the map, 
      - (compare-fields [rec values] : check map 'rec' against a sequence of values (ordered in correspondence with 'cmpFlds') 
      - (compare-maps rec1 rec2):  returns boolean
      - (map-comparators rec1 rec2): returns a map-comparator that can be used for sorting (returns -1, 0 or 1 instead of boolean) 
    The relevant keys are determined by cmpFlds." 
  ;; TODO: simplify this as function is way too complex
  ;;  (
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
            ;; should be equivalent to
            ;;  (= (select-keys rec1 cmpFlds) (select-keys rec2 cmpFlds))
            (compare-fields rec1 (get-fields rec2)))
            (map-comparator 
              ;; this is a comparator which means that it returns -1, 0 or 1  (compare-maps returns a boolean)
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


(defn keywordize 
  "Keywordize the keys of a map."
  ([m] (keywordize m false)) 
  ([m toLower]
    {:pre [(and (or (map? m) (isa? (class m) java.util.Properties)) 
                (isa? (class true) java.lang.Boolean))]}
    (let [transform (if toLower 
                      (comp keyword str/lower-case name)
                      keyword)]
    (zipmap (map transform (keys m))
            (vals m)))))

;; TODO: see whether these convertors should be combined with convertors from vinzi.anchorModel.extractModel.convertors and vinzi.eis.scipio.extractModel
;;  currently only used in csv an olap4Clj
;;  Should be merged with vString/convert-type-params, which looks like a cleaner implementation. However, this version is more
;;  efficient as it does key-word type mapping once
;;   cleansing of types (done in get-convertor) is not performed by vString/convert-type-params
(defn get-map-type-convertor 
  "Takes a map where values are the destination types and returns a function that 
  takes a map containing strings as values returns a new map with the string-values 
  converted to the types defined in type-map.
   (type-map should contains types as returned by sql (meta-data))." 
  ([typeMap] (get-map-type-convertor typeMap false))
  ([typeMap keepKeys]
  {:pre [(map? typeMap)]}
  (letfn [(get-convertor [tp]
                         (let [tp (if (string? tp)
                                    (let [tp (-> tp (str/trim) (str/lower-case))]
                                      (if (.startsWith tp "varchar") 
                                        :string
                                        (if (.startsWith tp "timestamp")
                                          :timestamp
                                          (keyword tp))))
                                    (if (keyword? tp)
                                      tp   ;; TODO: change keyword to lower-case
                                      (throw (Exception. (str "Obtained type " tp " which is not a string or keyword")))))
                               ;; case-statement can not handle keywords containing spaces, so translate them
                               tp (if (= tp (keyword "character varying"))
                                    :string 
                                    (if (= tp (keyword "double precision"))
                                      :double
                                      tp))]
                         (case tp
                           (:integer :int) 
                               (fn [x] (if-let [x (and x (str/trim x))]
                                         (if (seq x)   ;; prevent nil string from being processed
                                           (Integer/parseInt x)
                                           0)    ;; default value is 0  (no NaN availabel)
                                         0))
                           (:real :double) 
                               (fn [x] (if-let [x (and x (str/trim x))]
                                         (if (seq x)
                                           ;; second str/trim not needed
                                           ;; shouldn't we pass nil stead of NaN?
                                           (if (re-find #"(?i)null|n/a" x) Double/NaN (Double/parseDouble (str/trim x)))
                                           Double/NaN)   ;; empty string translates to NaN
                                         Double/NaN))    ;; nil translates to NaN
                           (:string :text)
                               (fn [x] x)
                           (:keyword)
                               (fn [x] (keyword (str x)))
                           (:date )  
                               vDate/convert-to-date
                           (:timestamp ) 
                               vDate/convert-to-timestamp
                           (:boolean :bool) (fn [x] (case (str/lower-case (str/trim x))
                                                          ("t" "true") true
                                                          ("f" "false") false
                                                          (throw (Exception. (str "(vMap/get-map-type-convertor): Can not map value: " x " to a boolean")))))
                           (throw (Exception. (str "Unknown type: " tp))))))]
         (let [convertors (map (fn[[k v]] (vector k (get-convertor v))) (seq typeMap))]
           ;;(pprint convertors)
           (fn [m]
             (let [initial (if keepKeys 
                             (if (= (type m) java.util.Properties)
                               (into {} m)   ;; translate java.util.Properities to clojure.lang.PersistentArrayMap
                               m)
                             {})]
               (into initial (map (fn [[k conv]] [ k (conv (get m k))]) convertors))))))))


;; Usually (small) maps have sorted keys when created in one go. However, sometimes this order gets lost
;; on maps making comparison fail. 
(defn make-sorted-map 
  "Prepare a sorted-map of x where the ordering of keys is given by the lexical order of the keys."
  [x]
  (apply sorted-map-by compare (interleave (keys x) (vals x))))

(defn map-compare 
  "Map-compare does a comparison of two maps by first sorting them. For small maps this is not needed, but for larger maps, or maps that have been created/modified in multiple stages the ordering of keys is not quaranteed, and comparison might fail."
  [x y]
     (let [srt-map #(apply sorted-map (apply concat (seq %)))]
       (= (srt-map x) (srt-map y))))


 (defn checked-add-kv 
   "A key is added if it does not exist in the hashmap. Otherwise an exception is raised"
   ([cumm kv ] (checked-add-kv cumm kv ""))
   ([cumm kv errPrefix]
   ;; 
   (let [lpf "(checked-add-kv): "
         k (first kv)]
     (if (cumm k)
       (vExcept/throw-except lpf  errPrefix  "key=" k " is already in map " cumm)
       (conj cumm kv)))))

(defn checked-merge 
  "Merge add into cumm and raise exception on duplicate keys (overwritting keys)."
  [cumm add]
  (reduce checked-add-kv cumm add))

(defn kvSeq-to-hashmap 
  "Prepare a hash-map from a key-value sequence. Duplicate keys are not allowed."
  [kvSeq]
    (reduce checked-add-kv {} kvSeq)) 

(defn hmSeq-to-hashmap 
  "Prepare a hash-map from a sequence of hashmaps using kkey as key and kval as value in the new hashmap.
   So we basically take two columns out of a dataset (xrel) and interpret them as a key-value pair."
  ;; Translation of a sequence of vectors to hashmaps is in vCsv/csv-to-map
  [hmSeq kkey kval]
  (kvSeq-to-hashmap (map #(vector (% kkey) (% kval)) hmSeq)))


(comment 
  ;;  map-compare using sorted map, however, example shows it is not needed (at least not for small vectors)
(defn map-compare [x y]
     (let [srt-map #(apply sorted-map (apply concat (seq %)))]
       (= (srt-map x) (srt-map y))))
  
;=> (map-compare {:a 1 :b 2} {:b 2 :a 1})
;true
;=> (= {:a 1 :b 2} {:b 2 :a 1})
;true

;; not needed?
(def a (reduce conj {} (map #(vector (keyword (str "lbl_" %)) %) (range 100))))
(def b (assoc (reduce conj {} (map #(vector (keyword (str "lbl_" %)) %) (reverse (range 100))))
              :lbl_82 82))
(= a b)
true

;Code of mapEquals suggests that size map-comparison does the key-lookup
;https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/APersistentMap.java
;		static public boolean mapEquals(IPersistentMap m1, Object obj){
;		if(m1 == obj) return true;
;		if(!(obj instanceof Map))
;		return false;
;		Map m = (Map) obj;
;		
;		if(m.size() != m1.count())
;		return false;
;		
;		for(ISeq s = m1.seq(); s != null; s = s.next())
;		{
;		Map.Entry e = (Map.Entry) s.first();
;		boolean found = m.containsKey(e.getKey());
;		
;		if(!found || !Util.equals(e.getValue(), m.get(e.getKey())))
;		return false;
;		}
;		
;		return true;
;		}



)  ;; end map-compare 


