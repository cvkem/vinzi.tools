(ns vinzi.tools.test.vXml
  (:use clojure.test)
  (:require
    [clojure
     [string :as str]
     [xml :as xml]]
    [vinzi.tools
     [vDateTime :as vDate]
     [vXml :as vXml]]))



(def testXml1 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
               <top-level>
                  <sub name=\"A\">
                  </sub>
                  <sub name=\"B\"/>
               </top-level>")

(def keyMap1  {:top-level 
                       {:keyMap {:sub 
                                 {:idAttr :name}}}})

(def resXml1 [:top-level {:A {} :B {}}])

(def testXml2nameConflict "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
               <top-level>
                  <sub name=\"A\">
                  </sub>
                  <sub name=\"A\">
                  </sub>
               </top-level>")

(def testXml3attributeConflict "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
               <top-level A=\"attribute conflict\">
                  <sub name=\"A\">
                  </sub>
                  <sub name=\"B\">
                  </sub>
               </top-level>")

;; one additional nested level 
(def testXml4 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
               <top-level>
                  <sub name=\"A\">
                      <subsub theKey=\"AA\"/>
                  </sub>
                  <sub name=\"B\">
                  </sub>
               </top-level>")

(def keyMap4  {:top-level 
                       {:keyMap {:sub {:idAttr :name
                                       :keyMap {:subsub {:idAttr :theKey}}}}}})

(def resXml4 [:top-level {:A {:AA {}} :B {}}])


(def testXml5 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
               <top-level>
                  <sub name=\"A\">
                  </sub>
                  <SUB theName=\"A2\"/>
                  <sub name=\"B\"/>
               </top-level>")

(def keyMap5  {:top-level 
                       {:keyMap {:sub {:idAttr :name}
                                 :SUB {:idAttr :theName}}}})

(def resXml5 [:top-level {:A {} :A2 {} :B {}}])


;; one additional nested level 
(def testXml6 "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
               <top-level>
                  <sub name=\"A\">
                      <subsub theKey=\"AA\"/>
                  </sub>
                  <SUB2 name=\"A2\">
                      <subsub theKey=\"AA\"/>
                  </SUB2>
                  <sub name=\"B\">
                  </sub>
               </top-level>")

(def keyMap6  {:top-level 
                       {:keyMap {:sub {:idAttr :name
                                       :keyMap {:subsub {:idAttr :theKey}}}
                                 :SUB2 {:idAttr :name
                                       :keyMap {:subsub {:idAttr :theKey}}}}}})

(def resXml6 [:top-level {:A {:AA {}} :A2 {:AA {}}:B {}}])


(deftest testxml
  (let [fName "/tmp/tstxml1.xml"]
    (spit fName testXml1)
    ;; from file
    (is (= (vXml/xml-file-to-hashmap fName keyMap1) resXml1))
    ;; from string
    (is (= (vXml/xml-str-to-hashmap testXml1 keyMap1) resXml1))
    ;; involves a key-conflict (duplicate key or key vs existing attribute)
    (is (thrown? Exception (vXml/xml-str-to-hashmap testXml2nameConflict keyMap1)))
    (is (thrown? Exception (vXml/xml-str-to-hashmap testXml3attributeConflict keyMap1)))

    (is (= (vXml/xml-str-to-hashmap testXml4 keyMap4) resXml4))
    (is (= (vXml/xml-str-to-hashmap testXml5 keyMap5) resXml5))
    (is (= (vXml/xml-str-to-hashmap testXml6 keyMap6) resXml6))
    ))