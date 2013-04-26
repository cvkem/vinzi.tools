(ns vinzi.tools.vXml
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [xml :as xml]
             [set :as set]]
            [vinzi.tools [vExcept :as vExcept]]))

(def validTagDescrKeys #{:keyMap :idAttr :keepId})

(def ^:dynamic tagIt true)

(defn set-tagIt [value] 
  (def tagIt value))

;; this routine is a small xml-parser that also does some basic validation of the xml.
;; currently it only checks the existence of the tags in the xmlDef
(defn xml-to-hashmap 
  "Translation maps an xml-tree to a clojure structure of nested hash-maps using a xmlDef structure. The xmlDef is used
     1. to define what attribute is used to name a tag in the hashmap (needed if the same tag appears multiple time (at the same level)
     2. validates that all tags are defined (when forceTagDef is true (=default)).
   This function puts contents items as hashmap into the attribute-map (loses ordering of content items,
    and throwing an exception when a content-item collides/overwrites an existing attribute).
    A (level of) a keyMap contains tags as keys and tagDescriptors as value. A tagDescriptor is a hash-map containing
     an :idAttr (used instead of tag as name for the element) and a keyMap for the next level
     An example of a xmlDef is:
               {:entity {:idAttr :group
                            :keyMap {:location  {:idAttr :name}}}}
    Which corresponds to an xml file with top-level <entity>, the idAttr :group is use to get the key for the (group of) locations
        as defined by :location records within this location. Each location use of idAttr 'name' as key in the hashmap contains a locs of group."
  ([xml xmlDef] (xml-to-hashmap xml xmlDef true))
  ([xml xmlDef forceTagDef]
    (let [lpf "(xml-to-hashmap): " 
          {:keys [tag attrs content]} xml
          tagDescr (when xmlDef (xmlDef tag))
          _  (when (and forceTagDef (nil? tagDescr))
               (vExcept/throw-except lpf "the tag: " tag " is not defined in xmlDef. Correct this, or set parameter forceTagDef to false."
                                     "\n\tCurrent xmlDef: " (with-out-str (pprint xmlDef))))
          {:keys [idAttr keyMap keepId]} tagDescr
;          _ (println "Parsing tag " tag " with tagDescr: " tagDescr)
          _ (when-let [unknownKeys (seq (set/difference (set (keys tagDescr)) validTagDescrKeys))]
              (vExcept/throw-except lpf "Keymap should only contain the keys: " validTagDescrKeys ", also observed keys: " unknownKeys))
          [nme attrs] (if idAttr 
                        [(keyword (idAttr attrs)) (if keepId attrs (dissoc attrs idAttr))]
                      [tag attrs])
          attrs (if (and tagIt (not (:tag attrs))) (assoc attrs :tag tag) attrs)   ;; adds :tag if it does not exist already in the record (unless it has value nil)
          _ (println  "\t using name "  nme (str (when idAttr (str " based on attribute " idAttr))))
          check-conj (fn [cumm kv]
                       ;; 
                       (let [k (first kv)]
                         (if (k cumm)
                           (vExcept/throw-except lpf "key=" k " is already present in map for " nme "\n\t map contains: " cumm
                                              "\n\t (notice xml is processed depth-first, so there might also be unnoticed errors higher up in the tree)")
                           (conj cumm kv))))
          ]
      (println "processing with tag=" tag " nme=" nme " and attrs="attrs)
      (when (nil? nme)
        (vExcept/throw-except lpf " No name obtained for tag=" tag " using idAttr=" idAttr " within attributes=" attrs))
      [nme (if content
             ;; TODO:  replace conj with a function that warns/errors if a key already exists.
             (reduce check-conj (if attrs attrs {}) (map #(xml-to-hashmap % keyMap forceTagDef) content))
             attrs)])))


(defn xml-file-to-hashmap
  "Turn a file with name 'fName' to a hashmap (see xml-to-hashmap for the transformation)"
  [fName keyMap]
  (xml-to-hashmap (xml/parse fName) keyMap))

(defn xml-str-to-hashmap 
  "Turn a string to a hashmap (see xml-to-hashmap for the transformation)"
  [s keyMap]
  (xml-to-hashmap (xml/parse  (java.io.ByteArrayInputStream. (.getBytes s))) keyMap))


