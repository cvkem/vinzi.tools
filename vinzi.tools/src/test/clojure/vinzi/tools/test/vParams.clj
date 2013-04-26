(ns vinzi.tools.test.vParams
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vDateTime :as vDate]
     [vParams :as vPar]]))




(deftest commandline-overrides
    (are [inp props expect] (= (vPar/commandline-override inp :a props) expect)
         ["a=test"] {:a :x} {:a :test}
         ["a=test"] {:a "x"} {:a "test"}
         ["b=x" "a=test" "status=5"] {:a :x} {:a :test}
         ["a=5"] {:a 4} {:a 5}
         ["a=-5"] {:a 4} {:a -5}
         ["a=5"] {:a (int 4)} {:a (int 5)}
         ["a=5"] {:a 4.0} {:a 5.0}
         ["a=-5.0"] {:a 4.0} {:a -5.0}
  ))


(deftest commandline-overrides-all
    (are [inp props expect] (= (vPar/commandline-override-all inp props) expect)
         ["a=test"] {:b 1 :a :x} {:b 1 :a :test}
         ["a=test" "b=2"] {:a "x" :b 1} {:b 2 :a "test"}
         ["a=test" "b=2" "c=100"] {:a "x" :b 1 :c 0} {:b 2 :a "test" :c 100}
  ))