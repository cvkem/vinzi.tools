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