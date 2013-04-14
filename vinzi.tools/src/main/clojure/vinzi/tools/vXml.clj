(ns vinzi.tools.vXml
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [xml :as xml]]
            [vinzi.tools [vExcept :as vExcept]]))


(defn xml-to-hashmap 
  "Translation outine that maps an xml-tree to a clojure structure of nested hash-maps using a keyMap structure.
   This function puts contents items as hashmap into the attribute-map (loses ordering of content items,
    and throwing an exception when a content-item collides/overwrites an existing attribute).
     An example of a keyMap is:
               {:locations 
                       {:keyMap {:location 
                                 {:idAttr :name}}}}
    Which corresponds to an xml file with to-level <locations> containing a sequence of nested :location records,
        using the values of idAttr 'name' a key in the hashmap."
  [xml keyMap]
  (let [lpf "(xml-to-hashmap): " 
        {:keys [tag attrs content]} xml
        _ (println "process tag: " tag)
        {:keys [idAttr keyMap]} (when keyMap (keyMap tag))
        [nme attrs] (if idAttr 
                      [(keyword (idAttr attrs)) (dissoc attrs idAttr)]
                      [tag attrs])
        check-conj (fn [cumm kv]
                     ;; 
                    (let [k (first kv)]
                      (if (k cumm)
                        (vExcept/throw-except lpf "key=" k " is already present in map for " nme "\n\t map contains: " cumm)
                        (conj cumm kv))))
        ]
    (println "processing with tag=" tag " nme=" nme " and attrs="attrs)
    (when (nil? nme)
      (vExcept/throw-except lpf " No name obtained for tag=" tag " using idAttr=" idAttr " within attributes=" attrs))
    [nme (if content
           ;; TODO:  replace conj with a function that warns/errors if a key already exists.
           (reduce check-conj (if attrs attrs {}) (map #(xml-to-hashmap % keyMap) content))
           attrs)]))


(defn xml-file-to-hashmap
  "Turn a file with name 'fName' to a hashmap (see xml-to-hashmap for the transformation)"
  [fName keyMap]
  (xml-to-hashmap (xml/parse fName) keyMap))

(defn xml-str-to-hashmap 
  "Turn a string to a hashmap (see xml-to-hashmap for the transformation)"
  [s keyMap]
  (xml-to-hashmap (xml/parse  (java.io.ByteArrayInputStream. (.getBytes s))) keyMap))


