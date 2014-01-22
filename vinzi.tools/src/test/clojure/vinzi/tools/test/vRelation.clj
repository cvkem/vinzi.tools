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
       (list (first joinA)) [:k] :jk  {:jk {:x \a}, :k 1}
       joinA [:k] :jk    '({:jk {:x \a}, :k 1} {:jk {:x "AA"}, :k 1} {:jk {:x \b}, :k 2}))
     )



(def list_x  '({:key 1 :x 1}
               {:key 2 :x 2}))
(def set_x (set list_x))

(def list_y '({:key 2 :y 2}
               {:key 3 :y 3}))
(def set_y (set list_y))

(def list_z '({:key 1 :z 1}
              {:key 2 :z 2}
               {:key 3 :z 3}))
(def set_z (set list_z))


(def res1 #{{:key 2 
            :X {:x 2} 
            :Y {:y 2}}})

(def res1ano #{{:key 2 
                :s0 {:x 2} 
                :s1 {:y 2}}})

(def res1_3 #{{:key 2 
              :X {:x 2} 
              :Y {:y 2}
              :Z {:z 2}}})


(deftest test-natural-inner-join
  (are [l r xrelKeys res]  (= (vRel/natural-inner-join [l r] [:key] xrelKeys) res)
        list_x list_y [:X :Y]   res1
        set_x set_y   [:X :Y]   res1
        list_x set_y  [:X :Y]   res1
        set_x list_y  [:X :Y]   res1
       ;; reverse options 
        list_y list_x [:Y :X]   res1
        ;; no names provided
        list_x list_y nil   res1ano
        set_x set_y   nil   res1ano
       )
   (are [l m r xrelKeys res]  (= (vRel/natural-inner-join [l m r] [:key] xrelKeys) res)
       ;; arity three tests by adding list
        list_z list_x list_y [:Z :X :Y]   res1_3   
        list_x list_z list_y [:X :Z :Y]   res1_3   
        list_x list_y list_z [:X :Y :Z]   res1_3   
       ;; arity three tests by adding set
        set_z list_x list_y [:Z :X :Y]   res1_3   
        list_x set_z list_y [:X :Z :Y]   res1_3   
        list_x list_y set_z [:X :Y :Z]   res1_3   
        )
   (are [l r res]  (= (vRel/natural-inner-join (list l r) [:key]) res)
        ;; no names provided
        list_x list_y res1ano
        set_x set_y   res1ano
        ))

 (def res1left #{;; first item
                 {:key 1 
                  :X {:x 1} } 
                 ;; second item
                 {:key 2 
                  :X {:x 2} 
                  :Y {:y 2}}}) 
 (def res1left_3 #{;; first item
                   {:key 1 
                    :X {:x 1}
                    :Z {:z 1} }     ;; extre entry for third set
                   ;; second item
                   {:key 2 
                    :X {:x 2} 
                    :Y {:y 2}
                    :Z {:z 2}}})   ;; extra entry for third set

;; also used for the reversed left-join
 (def res1right #{;; first item
                 {:key 3 
                  :Y {:y 3} } 
                 ;; second item
                 {:key 2 
                  :X {:x 2} 
                  :Y {:y 2}}}) 

(deftest test-left-outer-join
  (are [l r xrelKeys res]  (= (vRel/left-outer-join [l r] [:key] xrelKeys) res)
        list_x list_y [:X :Y]   res1left
        set_x set_y   [:X :Y]   res1left
        list_x set_y  [:X :Y]   res1left
        set_x list_y  [:X :Y]   res1left
       ;; reverse options 
        list_y list_x [:Y :X]   res1right
       )
   (are [l m r xrelKeys res]  (= (vRel/left-outer-join [l m r] [:key] xrelKeys) res)
       ;; arity three tests by adding list
        list_x list_z list_y [:X :Z :Y]   res1left_3   
        list_x list_y list_z [:X :Y :Z]   res1left_3   
       ;; arity three tests by adding set
        list_x set_z list_y [:X :Z :Y]   res1left_3   
        list_x list_y set_z [:X :Y :Z]   res1left_3   
        )
  )




 (def res1right_3 #{;; first item
                    {:key 3 
                     :Y {:y 3}
                     :Z {:z 3} } 
                    ;; second item
                    {:key 2 
                     :X {:x 2} 
                     :Y {:y 2}
                     :Z {:z 2}}}) 

(deftest test-right-outer-join
  (are [l r xrelKeys res]  (= (vRel/right-outer-join [l r] [:key] xrelKeys) res)
        list_x list_y [:X :Y]   res1right
        set_x set_y   [:X :Y]   res1right
        list_x set_y  [:X :Y]   res1right
        set_x list_y  [:X :Y]   res1right
       ;; reverse options 
        list_y list_x [:Y :X]   res1left
       )
   (are [l m r xrelKeys res]  (= (vRel/right-outer-join [l m r] [:key] xrelKeys) res)
       ;; arity three tests by adding list
        list_z list_x list_y [:Z :X :Y]   res1right_3   
        list_x list_z list_y [:X :Z :Y]   res1right_3   
       ;; arity three tests by adding set
        set_z list_x list_y [:Z :X :Y]   res1right_3   
        list_x set_z list_y [:X :Z :Y]   res1right_3   
        )
  ) 

 (def res1full #{;; first item
                 {:key 3 
                  :Y {:y 3} } 
                 ;; second item
                 {:key 2 
                  :X {:x 2} 
                  :Y {:y 2}}
                 {:key 1
                  :X {:x 1}}}) 
 (def res1full_3 #{;; first item
                   {:key 3 
                    :Y {:y 3}
                    :Z {:z 3} } 
                   ;; second item
                   {:key 2 
                    :X {:x 2} 
                    :Y {:y 2}
                    :Z {:z 2}}
                   {:key 1
                    :X {:x 1}
                    :Z {:z 1}}}) 



(deftest test-full-outer-join
  (are [l r xrelKeys res]  (= (vRel/full-outer-join [l r] [:key] xrelKeys) res)
        list_x list_y [:X :Y]   res1full
        set_x set_y   [:X :Y]   res1full
        list_x set_y  [:X :Y]   res1full
        set_x list_y  [:X :Y]   res1full
       ;; reverse options 
        list_y list_x [:Y :X]   res1full
       )
   (are [l m r xrelKeys res]  (= (vRel/full-outer-join [l m r] [:key] xrelKeys) res)
       ;; arity three tests by adding list
        list_z list_x list_y [:Z :X :Y]   res1full_3   
        list_x list_z list_y [:X :Z :Y]   res1full_3   
       ;; arity three tests by adding set
        set_z list_x list_y [:Z :X :Y]   res1full_3   
        list_x set_z list_y [:X :Z :Y]   res1full_3   
        )
  ) 

