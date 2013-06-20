(ns vinzi.tools.test.vRelation
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vRelation :as vRel]
     [vMap :as vMap]]))



;;; testcode
;(def diffA '({:k 1  :x \a }
;              {:k 1 :x "AA"}
;              {:k 2  :x \b }))
;
;(def diffB '({:k 1  :x \a }
;              {:k 1 :x "BB"}
;              {:k 2  :x \b }))

(def joinA '({:k 1  :x \a }
              {:k 1 :x "AA"}
              {:k 2  :x \b }))

(def joinB '({:k 1  :y 1 }
              {:k 2  :y 2 }
              {:k 2  :y 2.1 }))
;=> (split-recs (first joinA) [:k] :jk)
;{:jk {:x \a}, :k 1}
;=> (split-recs joinA [:k] :jk)
;({:jk {:x \a}, :k 1} {:jk {:x "AA"}, :k 1} {:jk {:x \b}, :k 2})



(deftest split-rec-test
  (are [rec ks ok res] (= (vRel/split-recs rec ks ok ))
       (first joinA) [:k] :jk  {:jk {:x \a}, :k 1}
       joinA [:k] :jk    '({:jk {:x \a}, :k 1} {:jk {:x "AA"}, :k 1} {:jk {:x \b}, :k 2}))
     )