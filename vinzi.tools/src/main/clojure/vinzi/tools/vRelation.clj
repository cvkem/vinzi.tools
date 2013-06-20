(ns vinzi.tools.vRelation
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
   (:require [clojure
              [string :as str]
              [set :as set]]
             [vinzi.tools
              [vMap :as vMap]]))





(defn split-recs 
  "Transform a record (map or xrel) by turning all 'kKeys' into a new record and storing all other data in a separate
   map under the 'otherKeys'."
  [rec kKeys otherKey]
  {:pre [(or (map? rec) (sequential? rec)) (sequential? kKeys) (keyword? otherKey)]}
  (let [split-rec-aux (fn [rec]
                        (let [nk (-> rec
                                 (keys)
                                 (set)
                                 (set/difference  (set kKeys))
                                 (seq ))]
                        (-> (select-keys rec kKeys)
                          (assoc otherKey (select-keys rec nk)))))]
    (if (map? rec)
      (split-rec-aux rec)
      (map split-rec-aux rec)))) 


;(defn join-recs
;  "Join the relations 'xrel' and 'yrel' on 'kKeys' and store the other keys under 'xOther' and 'yOther'."
;  ([xrel yrel kKeys] (join-recs xrel yrel kKeys :xOther :yOther))
;  ([xrel yrel kKeys]
;  (let []
;  
;  )))

