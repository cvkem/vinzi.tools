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



(deftest split-recs-test
  (are [rec ks ok res] (= (vRel/split-recs rec ks ok ))
       (list (first joinA)) [:k] :jk  {:jk {:x \a}, :k 1}
       joinA [:k] :jk    '({:jk {:x \a}, :k 1} {:jk {:x "AA"}, :k 1} {:jk {:x \b}, :k 2}))
     )

(deftest extract-recs-test
  (are [xrel insKeys remKeys res]  (= (set (vRel/extract-recs xrel insKeys remKeys)) res)
       (list {:a 1 :b {:x 2}})  '(:b)  nil  #{{:a 1 :x 2}}
       (list {:a 1 :b {:x 2}})  '(:b)  ()  #{{:a 1 :x 2}}
       (list {:a 1 :b {:x 2}} {:a -1 :b {:x -2}})  '(:b)  nil  
           #{{:a 1 :x 2} {:a -1 :x -2}}
       (list {:a 1 :b {:x 2} :c {:y :a}} {:a -1 :b {:x -2} :c {:y :b}})  '(:b)  nil  
           #{{:a 1 :x 2 :c {:y :a}} {:a -1 :x -2 :c {:y :b}}}
       (list {:a 1 :b {:x 2} :c {:y :a}} {:a -1 :b {:x -2} :c {:y :b}})  '(:b)  '(:c)  
           #{{:a 1 :x 2 } {:a -1 :x -2 }}
       ))



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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Some more tests with string keys
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def list_x2  '({:key "A" :x 1}
               {:key "B" :x 2}))
(def set_x2 (set list_x2))

(def list_y2 '({:key "B" :y 2}
               {:key "C" :y 3}))
(def set_y2 (set list_y2))

(def list_z2 '({:key "A" :z 1}
              {:key "B" :z 2}
               {:key "C" :z 3}))
(def set_z2 (set list_z2))


(def res2 #{{:key "B" 
            :X {:x 2} 
            :Y {:y 2}}})

(deftest test-natural-inner-join-2
  (are [l r xrelKeys res]  (= (vRel/natural-inner-join [l r] [:key] xrelKeys) res)
        list_x2 list_y2 [:X :Y]   res2
        set_x2 set_y2   [:X :Y]   res2
        list_x2 set_y2  [:X :Y]   res2
        set_x2 list_y2  [:X :Y]   res2
       ;; reverse options 
        list_y2 list_x2 [:Y :X]   res2
        ;; no names provided
;;        list_x2 list_y2 nil   res2ano
 ;;       set_x2 set_y2   nil   res2ano
       ))

 (def res2left #{;; first item
                 {:key "A" 
                  :X {:x 1} } 
                 ;; second item
                 {:key "B" 
                  :X {:x 2} 
                  :Y {:y 2}}}) 

;; also used for the reversed left-join
 (def res2right #{;; first item
                 {:key "C" 
                  :Y {:y 3} } 
                 ;; second item
                 {:key "B" 
                  :X {:x 2} 
                  :Y {:y 2}}}) 

(deftest test-left-outer-join-2
  (are [l r xrelKeys res]  (= (vRel/left-outer-join [l r] [:key] xrelKeys) res)
        list_x2 list_y2 [:X :Y]   res2left
        set_x2 set_y2   [:X :Y]   res2left
        list_x2 set_y2  [:X :Y]   res2left
        set_x2 list_y2  [:X :Y]   res2left
       ;; reverse options 
        list_y2 list_x2 [:Y :X]   res2right
       )
  )


 (def res2full #{;; first item
                 {:key "C" 
                  :Y {:y 3} } 
                 ;; second item
                 {:key "B" 
                  :X {:x 2} 
                  :Y {:y 2}}
                 {:key "A"
                  :X {:x 1}}}) 

(deftest test-full-outer-join-2
  (are [l r xrelKeys res]  (= (vRel/full-outer-join [l r] [:key] xrelKeys) res)
        list_x2 list_y2 [:X :Y]   res2full
        set_x2 set_y2   [:X :Y]   res2full
        list_x2 set_y2  [:X :Y]   res2full
        set_x2 list_y2  [:X :Y]   res2full
       ;; reverse options 
        list_y2 list_x2 [:Y :X]   res2full
       ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Some more tests with multiple string keys
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def list_x3  '({:key "A" :key2 :a :x 1}
               {:key "B" :key2 :b :x 2}))
(def set_x3 (set list_x3))

(def list_y3 '({:key "B" :key2 :b :y 2}
               {:key "C" :key2 :c :y 3}))
(def set_y3 (set list_y3))

(def list_z3 '({:key "A" :key2 :a :z 1}
              {:key "B" :key2 :b :z 2}
               {:key "C" :key2 :c :z 3}))
(def set_z3 (set list_z3))


(def res3 #{{:key "B" 
             :key2 :b
            :X {:x 2} 
            :Y {:y 2}}})

(deftest test-natural-inner-join-3
  (are [l r xrelKeys res]  (= (vRel/natural-inner-join [l r] [:key :key2] xrelKeys) res)
        list_x3 list_y3 [:X :Y]   res3
        set_x3 set_y3   [:X :Y]   res3
        list_x3 set_y3  [:X :Y]   res3
        set_x3 list_y3  [:X :Y]   res3
       ;; reverse options 
        list_y3 list_x3 [:Y :X]   res3
        ;; no names provided
;;        list_x2 list_y2 nil   res2ano
 ;;       set_x2 set_y2   nil   res2ano
       ))

 (def res3left #{;; first item
                 {:key "A" :key2 :a
                  :X {:x 1} } 
                 ;; second item
                 {:key "B"  :key2 :b
                  :X {:x 2} 
                  :Y {:y 2}}}) 

;; also used for the reversed left-join
 (def res3right #{;; first item
                 {:key "C" :key2 :c
                  :Y {:y 3} } 
                 ;; second item
                 {:key "B" :key2 :b
                  :X {:x 2} 
                  :Y {:y 2}}}) 

(deftest test-left-outer-join-3
  (are [l r xrelKeys res]  (= (vRel/left-outer-join [l r] [:key :key2] xrelKeys) res)
        list_x3 list_y3 [:X :Y]   res3left
        set_x3 set_y3   [:X :Y]   res3left
        list_x3 set_y3  [:X :Y]   res3left
        set_x3 list_y3  [:X :Y]   res3left
       ;; reverse options 
        list_y3 list_x3 [:Y :X]   res3right
       )
  )


 (def res3full #{;; first item
                 {:key "C" 
                  :key2 :c
                  :Y {:y 3} } 
                 ;; second item
                 {:key "B" 
                  :key2 :b
                  :X {:x 2} 
                  :Y {:y 2}}
                 {:key "A"
                  :key2 :a
                  :X {:x 1}}}) 

(deftest test-full-outer-join-3
  (are [l r xrelKeys res]  (= (vRel/full-outer-join [l r] [:key :key2] xrelKeys) res)
        list_x2 list_y2 [:X :Y]   res2full
        set_x2 set_y2   [:X :Y]   res2full
        list_x2 set_y2  [:X :Y]   res2full
        set_x2 list_y2  [:X :Y]   res2full
       ;; reverse options 
        list_y2 list_x2 [:Y :X]   res2full
       ))


