(ns vinzi.tools.test.vString
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vDateTime :as vDate]
     [vString :as vString]]))

(deftest test-replace-params
    ;; first test parameter-substitution standalone
    (let [code "line ${a}\n"
          expectRes "line 1\n"
          params {:a 1}
          res (vString/replace-params code params)]
      (is (= (:code res) expectRes)
          "case 1: Replacement returned the correct code")
      (is (= (:replCnt res) {:a 1})
          "case 1: Replacement returned the correct replCnt map"))
    (let [code "line ${a} \n${a}"
          expectRes "line 1 \n1"
          params {:a 1}
          res (vString/replace-params code params)]
      (is (= (:code res) expectRes)
          "case 2: Replacement returned the correct code")
      (is (= (:replCnt res) {:a 2})
          "case 2: Replacement returned the correct replCnt map")
      (let [params {:a 1 :b 2}
            res (vString/replace-params code params)]
        (is (= (:code res) expectRes)
            "case 3: Replacement retuned the correct code")
        (is (= (into (sorted-map) (seq (:replCnt res))) {:a 2 :b 0})
            "case 3: Replacement returned the correct replCnt map")))
   )




(deftest test-convert-type-params
  (are [pars typeMap res] (= (vString/convert-type-params pars typeMap) res)
       {:a "5"}  {:a :integer}   {:a 5}
       {:a "5"}  {:a :int}   {:a 5}
       {:a "5"}  {:a :long}   {:a 5}
       {:a "null"}  {:a :long}   {:a nil}
       {:a "NULL"}  {:a :long}   {:a nil}
       {:a "5"}  {:b :integer}   {:a "5"}
       {:a "5"}  {:a :double}   {:a 5.0}
       {:a "5"}  {:a :real}   {:a 5.0}
       {:a "NuLL"}  {:a :double}   {:a nil}
 ;;      {:a "NaN"}  {:a :double}   {:a Double/NaN}
       {:a "2013-01-19"}  {:a :date}   {:a (vDate/convert-to-date "2013-01-19")}
       {:a "2013-01-19"}  {:a :timestamp}   {:a (vDate/convert-to-timestamp "2013-01-19")}
       {:a "TRUE"}        {:a :boolean}   {:a true}
       {:a "FaLsE"}        {:a :boolean}   {:a false})
       
     ;; Double/NaN  refuses comparison, so using a special test.
    (let [x (vString/convert-type-params {:a "NaN"} {:a :double})]
      (is (and (= (type (:a x)) Double) (= (str (:a x) "NaN"))))
       ))


(deftest map-nl-to-doubleStr 
  (are [nlStr dStr] (= (vString/map-nl-to-doubleStr nlStr) dStr)
       "123" "123"
       "   123" "123"
       "123 "  "123"
       "1,23"  "1.23"
       " 1.123,45" "1123.45"))