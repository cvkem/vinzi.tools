(ns vinzi.tools.vRelation
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
   (:require [clojure
              [string :as str]
              [set :as set]]
             [vinzi.tools
              [vExcept :as vExcept]
              [vMap :as vMap]]))





(defn split-recs 
  "Transform a xrelation (set or sequential) by turning all 'kKeys' into a new record 
  and storing all other data in a separate map under the 'otherKeys'.
  Used to prepare relations for a set/join"
  [recs kKeys otherKey]
  {:pre [(or (set? recs) (sequential? recs)) (sequential? kKeys) (keyword? otherKey)]}
  (let [split-rec-aux (fn [rec]
                        (let [nk (-> rec
                                 (keys)
                                 (set)
                                 (set/difference  (set kKeys))
                                 (seq ))]
                        (-> (select-keys rec kKeys)
                          (assoc otherKey (select-keys rec nk)))))]
;    (if (map? rec)
;      (split-rec-aux rec)
;      (map split-rec-aux rec)))) 
       (map split-rec-aux recs)))   



(defn- join-aux
   "do the natural inner join of the relations from xrels and store data under xrelkeys (defaults to :s0, :s1, ...)"
  [xrels matchKeys xrelKeys & opts ]
  {:pre [(sequential? xrels) (sequential? matchKeys) (or (nil? xrelKeys) (sequential? xrelKeys))]}
  (let [lpf "(vRelation/join-aux): "]
    (when  (< (count xrels) 2)
      (vExcept/throw-except lpf "A join requires at least two input relations."))
     (let [opts (into {:join-type :inner} (apply hash-map opts))
         ;  _ (println lpf " run with options: " opts)
           xrelKeys (if (seq xrelKeys) 
                        xrelKeys
                        (map #(keyword (str "s" %)) (range))) ;; get defaults
           spRecs (map #(split-recs %1 matchKeys %2) xrels xrelKeys)
           get-set-difference (fn [outerSet inner]
                                 (let [missingKeys (set/difference (set/project outerSet matchKeys)
                                                     (set/project inner matchKeys))]
                                   ;(println "missingKeys=" missingKeys)
                                   ;(println " select-keys-test=" (select-keys (first outerSet) matchKeys))
                                   (set/select #(missingKeys (select-keys % matchKeys)) (set outerSet))))
           get-inner-join (fn [spRecs] (reduce set/join spRecs))
           get-outer-join (fn [spRecs joinType]
                            (let [outer-join  (fn [cumm joinSet]
                                                (let [inner (set/join cumm joinSet)
                                                      restore (get-set-difference cumm inner)
                                                      restore2 (if (= joinType :full)
                                                                 (get-set-difference joinSet inner)
                                                                 #{})]
                                                  ;; restore records that have been left out of left set
                                                  ;(println " #cumm=" (count cumm) 
                                                  ;         " #inner=" (count inner)
                                                  ;         " #restore=" (count restore))
                                                  (set/union inner restore restore2)))] 
                              (reduce outer-join spRecs)))
         ]
       (case (:join-type opts)
        :inner  (get-inner-join spRecs)
        :left-outer (get-outer-join spRecs :left)
        :right-outer (get-outer-join (reverse spRecs) :left) ;; left join over reverse sequence --> right join
        :full-outer  (get-outer-join spRecs :full)
        (vExcept/throw-except lpf "unknown join-type " (:join-type opts)))))) 


(defn natural-inner-join 
   "do the natural inner join of the relations from xrels and store data under xrelkeys (defaults to :s0, :s1, ...)"
  ([xrels matchKeys]
    (join-aux xrels matchKeys nil :join-type :inner))
  ([xrels matchKeys xrelKeys]
    (join-aux xrels matchKeys xrelKeys :join-type :inner)))

(defn left-outer-join 
   "do the left-outer join of the relations from xrels and store data under xrelkeys (defaults to :s0, :s1, ...)"
  ([xrels matchKeys]
    (join-aux xrels matchKeys nil :join-type :left-outer))
  ([xrels matchKeys xrelKeys]
    (join-aux xrels matchKeys xrelKeys :join-type :left-outer)))

(defn right-outer-join 
   "do the right-outer join of the relations from xrels and store data under xrelkeys (defaults to :s0, :s1, ...)"
  ([xrels matchKeys]
    (join-aux xrels matchKeys nil :join-type :right-outer))
  ([xrels matchKeys xrelKeys]
    (join-aux xrels matchKeys xrelKeys :join-type :right-outer)))

(defn full-outer-join 
   "do the right-outer join of the relations from xrels and store data under xrelkeys (defaults to :s0, :s1, ...)"
  ([xrels matchKeys]
    (join-aux xrels matchKeys nil :join-type :full-outer))
  ([xrels matchKeys xrelKeys]
    (join-aux xrels matchKeys xrelKeys :join-type :full-outer)))

