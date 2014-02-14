(ns vinzi.tools.vNameParse
   (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
   (:require [clojure.string :as str]
             [vinzi.tools 
              [vExcept :as vExcept]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Namespace contains routines to parse a string representing a name to a hashmap
;;  with keys (:firstnames :infix :lastnames)
;;;;;;;;;;;;;;;;;;;;  NEXT STEPS/TODO;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  A better approache of name-parsing should also consider the - as a separator of first names
;;  or a separator of birth-surname and partner surname.
;;  The results should be a sequence of first names and a sequence of last-names+ infix.
;;  The steps would be: 
;;    Parse string from back to front, and:
;;    0. Replace '\s*-\s*'  by '-'  
;;    1. If string contains , assume lastname, first-name format (open issue, is suffix with last or first name?)
;;       otherwise assume <first-names> <infix <lstanem>-
;;    2. when next char is:
;;         a.  [ \.]space of dot check whether it is an infix (and add it)
;;              (otherwise switch to first-name parsing)
;;         b.  [\-]  assume a second surname needs to be parsed
;;         c.  [,]  you've just found the first-names instead of surnames. so 
;;             copy  surnames to firstnames and start parsing surnames again.  (or check this before analysis)
;;    3. Parse remainder of string as a series of firstnames.
;;
;;    See vinzi.eis.scipio.beaufortMatch.clj for a different implementation of matchers.
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;  fuzzy name-matcher  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Make a name-matcher that matches a sequence of names to another sequence and 
;;  generate the most likely match by:
;;     1. preparing alternative interpretations of strings with different likelyhood
;;     2. Finding the best covering match (lowest penalty) and thus using global information
;;         to decide on the best matching when multiple matches are present. 
;;         (under the assumption that a one-to-one mapping should be found. (multi-match is flag)
;;  This could also be used to introduced alternative mapping (for example for surnames containing infix
;;  or first names in one set and initials in the other set instead of the first names.
;;         
;;    See vinzi.eis.scipio.beaufortMatch.clj for an example of likelyhoods in (find-matches-anchorModel ).
;;
;;  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def infix
    ["v d"
     "van der"
     "van den"
     "van de"
     "van"
     "de la"
     "den"
     "de"
     "la"
     "le"
     "in het"
     "in 't"
     "vd"       ;; samenvoeging
     "v.d."
     "vden"  ;; geen formele afkorting
     "vder"    ;; samenvoeging van van der
     ])

(def infix-mask
  ;; greed match, so order prefixes in decreasing number of parts (and from long to shorter)
  ;; a function is applied over the list to replace "van den"  with " van\s*den " (to match one or more spaces between infix parts
  ;; and add the surrounding spaces and to match first and last parts of the name
  ((fn [infs]
     (let [make-mask (fn [inf]
                       (re-pattern (str "(?iu)^([\\w\\s\\-\\.]*) (" (str/replace inf #"\s+" "\\\\s+") ") ([\\w\\s\\-]*)$"))) ]
     (map make-mask infs))) infix))
  
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

(defn- rev-str 
  "Reverse the string."
  [s]
  (apply str (reverse s)))

(defn find-tail 
  "match the regular expression 're' backward over the string and 
   return the split string [head matched-tail]"
  [s re]
  (let [rs (rev-str s)
        tail (re-find re rs)]
    (if (seq tail)
      [(subs s 0 (- (count s) (count tail))) 
       (rev-str tail)]
      [s nil])))

(def rev-infix (map (juxt #(re-pattern (str "^" (str/replace (rev-str %) #"\." "\\\\.")) %)))

(defn find-infix-tail
  [s]
  (let [rs (rev-str s)
        infix (loop [ri rev-infix]
                (when ri
                  (let [[pat infix] (first ri)]
                    (if (re-find pat rs)
                      infix
                      (recur (next ri))))))]
    [(subs s 0 (- (count s) (count infix))) infix]))
        
(defn split-name2
  [s]
  (let [lpf "(split-name2): "
        normalize #(-> %
                       (str/replace #"\s*-\s*" "-")
                       (str/replace #"\s+" " "))
        first-last-order #(let [parts (->> (str/split % #",")
                                           (map str/trim ))]
                            (condp = (count parts)
                              1 %
                              2 (str (last parts) " " (first parts))
                              (vExcept/throw-except lpf " string contains more than one ','. Can not parse it)")))
        find-surname (fn [s]
                       (let [[head surname] (find-tail s #"^\w+")
                              lst (last head)]
                         (if (#{\. \space} lst)
                           (let [head (if (= lst \space) (apply str (drop-last head)) head)
                                 [head infix] (find-infix-tail head)]
                             [head {:surname surname
                                    :infix   infix}])
                           [head {:surname surname
                                  :infix   nil}])))
                             
        s (-> s
              (normalize )
              (first-last-order ))
        [s surName2] (find-surname s)
        [s surName1] (if (= (last s) \-)
                       (find-surname (apply str (drop-last s)))
                       [s nil])
        firstNames (str/split s #"\s")]
    {:firstNames firstNames
     :surnames   (if (seq surName1) [surName1 surName2] [surName2])}
    )))

        
