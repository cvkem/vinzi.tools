(ns vinzi.tools.vXml
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [xml :as xml]
             [set :as set]]
            [debug-repl [debug-repl :as dr]]
            [vinzi.tools 
             [vExcept :as vExcept]
             [vMap :as vMap]]))

(def validTagDescrKeys #{:keyMap :idAttr :keepId :allowContent :groupTags})

(def ^:dynamic tagIt true)

(defn set-tagIt [value] 
  (def tagIt value))

;; this routine is a small xml-parser that also does some basic validation of the xml.
;; currently it only checks the existence of the tags in the xmlDef
(defn xml-to-hashmap 
  "Translation maps an xml-tree to a clojure structure of nested hash-maps using a xmlDef structure. The xmlDef is used
     1. to define what attribute is used to name a tag in the hashmap (needed if the same tag appears multiple time (at the same level)
     2. validates that all tags are defined (when forceTagDef is true (= default)).
   This function puts contents items as hashmap into the attribute-map (loses ordering of content items,
    and throwing an exception when a content-item collides/overwrites an existing attribute).
    A (level of) a keyMap contains tags as keys and tagDescriptors as value. A tagDescriptor is a hash-map containing
     an :idAttr (used instead of tag as name for the element) and a keyMap for the next level
     An example of a xmlDef is:
               {:entity {:idAttr :group
                            :keyMap {:location  {:idAttr :name}}}}
    Which corresponds to an xml file with top-level <entity>, the idAttr :group is used to get the key for the (group of) locations
        as defined by :location records within this location. Each location use of idAttr 'name' as key in the hashmap contains a locs of group."
  ([xml xmlDef] (xml-to-hashmap xml xmlDef true))
  ([xml xmlDef forceTagDef]
    (let [lpf "(xml-to-hashmap): " 
          {:keys [tag attrs content]} xml
          tagDescr (when xmlDef (xmlDef tag))
          _  (when (and forceTagDef (nil? tagDescr))
               (vExcept/throw-except lpf "the tag: " tag " is not defined in xmlDef. Correct this, or set parameter forceTagDef to false."
                                     "\n\tCurrent xmlDef: " (with-out-str (pprint xmlDef))
                                     "\n\txml=" (with-out-str (pprint xml))))
          {:keys [idAttr keyMap keepId allowContent groupTags]} tagDescr
;          _ (println "Parsing tag " tag " with tagDescr: " tagDescr)
          _ (when-let [unknownKeys (seq (set/difference (set (keys tagDescr)) validTagDescrKeys))]
              (vExcept/throw-except lpf "KeyMap should only contain the keys: " validTagDescrKeys ", also observed keys: " unknownKeys))
          [nme attrs] (if idAttr 
                        [(keyword (idAttr attrs)) (if keepId attrs (dissoc attrs idAttr))]
                      [tag attrs])
          attrs (if (and tagIt (not (:tag attrs))) (assoc attrs :tag tag) attrs)   ;; adds :tag if it does not exist already in the record (unless it has value nil)
 ;;         _ (println  "\t using name "  nme (str (when idAttr (str " based on attribute " idAttr))))
          process-content (fn [attrs content]
                            ;; All none-xml elements stored under tag :xml-content, and the xml-elements are processed.
                            (let [errPrefix (str "Error for item: " nme "(notice xml is processed depth-first, so there might also be unnoticed errors higher up in the tree)")
                                  check-conj (fn [cumm kv] 
                                             ;;  (println "TMP: cumm= " cumm)
                                             ;;  (println "TMP: adding kv= "kv)
                                               (vMap/checked-add-kv cumm kv errPrefix))
;                                  check-conj (fn [cumm kv]
;                                               ;; 
;                                               (let [k (first kv)]
;                                                 (if (k cumm)
;                                                   (vExcept/throw-except lpf "key=" k " is already present in map for " nme "\n\t map contains: " cumm
;                                                                         "\n\t (notice xml is processed depth-first, so there might also be unnoticed errors higher up in the tree)")
;                                                   (conj cumm kv))))
                                  xmlElem (filter (complement string?) content)
                                  strElem (filter string? content)
                                  xmlElem (->> (map #(do
                                   ;;                    (println "xmlElem=" xmlElem)
                                                       (xml-to-hashmap % keyMap forceTagDef)) xmlElem)
                                            (reduce check-conj (if attrs attrs {}) ))
                                  ]
                              (if (seq strElem)
                                (if allowContent
                                  (assoc xmlElem :xml-content (vec strElem))
                                  (vExcept/throw-except lpf "content is not allowed for tag: " tag))
                                xmlElem)))
          ]
;;      (println "processing with tag=" tag " nme=" nme " and attrs="attrs)
      (when (nil? nme)
        (vExcept/throw-except lpf " No name obtained for tag=" tag " using idAttr=" idAttr " within attributes=" attrs))
      [nme (if content
              (process-content attrs content)
             attrs)])))


(defn xml-file-to-hashmap
  "Turn a file with name 'fName' to a hashmap (see xml-to-hashmap for the transformation)"
  [fName keyMap]
  (xml-to-hashmap (xml/parse fName) keyMap))

(defn xml-str-to-hashmap 
  "Turn a string to a hashmap (see xml-to-hashmap for the transformation)"
  [s keyMap]
  (xml-to-hashmap (xml/parse  (java.io.ByteArrayInputStream. (.getBytes s))) keyMap))


