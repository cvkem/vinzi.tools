(ns vinzi.tools.vNameParse
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
   (:require [clojure.string :as str]))


(def infix-mask
  ;; greed match, so order prefixes in decreasing number of parts (and from long to shorter)
  ;; a function is applied over the list to replace "van den"  with " van\s*den " (to match one or more spaces between infix parts
  ;; and add the surrounding spaces and to match first and last parts of the name
  ((fn [infs]
     (let [make-mask (fn [inf]
                       (re-pattern (str "(?iu)^([\\w\\s\\-\\.]*) (" (str/replace inf #"\s+" "\\\\s+") ") ([\\w\\s\\-]*)$"))) ]
     (map make-mask infs)))
    ["v d"
     "van der"
     "van den"
     "van de"
     "van"
     "de la"
     "de"
     "la"
     "le"
     "in het"
     "in 't"
     "vd"
     "vden"]))
  
(defn split-name 
  "Split a name in :firstnames, :infix and :lastnames. If there is no infix we split on whitespace and assume the last part
   is the last-names (separated by dashes -)."
  [s]
  (debug "(split-name): processing string: " s)
  (if-let [splitted (some identity (map #(re-find % s) infix-mask))]
    (let [[_ firstnames infix lastnames] splitted]
      {:firstnames firstnames
       :infix      infix
       :lastnames  lastnames})
    (let [splitted (str/split s #"\s+") 
          splitted (if (= (count splitted) 1)   ;; split at last . (of initials) if the string could not be split on spaces.
                     (-> (first splitted)
                       (str/split #"\.")
                       ((fn [spl] (if (> (count spl) 1) 
                                  (conj (vec (map #(str % ".") (drop-last spl))) (last spl))
                                  spl))))
                     splitted)]
      {:firstnames (-> (str/join " " (drop-last splitted))
                     (str/replace  #"\. " "."))   ;; after initials/dot no space
       :infix   ""
       :lastnames (last splitted)})
    ))

(defn add-initials-abbrev 
  "Add the initials and abbreviation to a record returned by split-name. 
      The abbreviation consist of first letters of firstname (upcase), infix (lowcase) and lastnames (upcase)." 
  [rec]
  (debug "(add-initials-abbrev): processing record: " rec)
  (let [initials (if (seq (:firstnames rec)) 
                   (-> (:firstnames rec)
                   (str/split #"[.\s+]" )
                   ((partial map #(str/upper-case (first %)))))
                   ())
        inf   (if (seq (:infix rec))
                (-> (:infix rec)
                  (str/split #"\s+")
;;                  ((partial (map #(str/replace % #"'t" "het"))))
                  ((partial map #(str/lower-case (first %))))
                  ((partial remove #(= % "'"))))
                "")
        lst (-> (:lastnames rec)
                   (str/split #"[\-\s]+" )
                   ((partial map #(str/upper-case (first %)))))
        abbrev (apply str (concat initials inf lst))
        initials (-> initials
                   (interleave (repeat "."))
                   ((partial apply str)))]
    (assoc rec :initials initials  :abbrev abbrev)))
