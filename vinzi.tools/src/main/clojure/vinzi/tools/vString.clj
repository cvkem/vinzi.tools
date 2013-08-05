(ns vinzi.tools.vString
  (:use	[clojure [pprint :only [pprint pp]]]
         [clojure [stacktrace :only [print-stack-trace root-cause]]]
	   [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [string :as str]
             [edn :as edn]]
            [vinzi.tools
             [vDateTime :as vDate]
             [vExcept :as vExcept]]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  functions for handeling string-parameters
;;    1. Replace parameters of shape ${  } in a string
;;    2. Do type conversion on strings according to a type-map  
;;       (special cases such as null, and NaN values handled too)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn escape-replace-str 
  "In replacement strings the $ character is interpretated and replaced with groupings.
   If you don't want this you need to escape the $-signs in the replacement string (unless they are already escaped) "
  [replStr]
  ;; special case for strings that start with $
  (str/replace replStr #"([^\\])\$|^\$" "$1\\\\\\$"))
  

(defn replace-params "Takes 'code' (multi-line string) and 
 performs the parameter substitution as defined in 'params' (keywords translated to strings).
 The result is a map containing :code (as a string) and 
 :replCnt (a map showing the number of replacements.)
   By default recognizes ${..} parameters, but different brackets can be provided as third parameter." 
  ([code params]  (replace-params code params '("\\$\\{" "\\}")))
  ([code params brackets]
  (letfn [(replace-par
            [{:keys [code replCnt]} [k v]]
            (let [lpf "(replace-par): "]
              (if (and code k (not (nil? v)))
                (let [pat (re-pattern (str (first brackets)  (name k) (second brackets)))
                      cnt (count (re-seq pat code))]
                  ;;(println "pattern =" pat)
                  (when (replCnt k)
                    ;; TODO: replace with one-liner vExcept/throw-except
                    (let [msg (str lpf "The parameter " k " is defined more than once")]
                      (error msg)
                      (throw (Exception. msg))))
                  (debug lpf "execute replace " pat " --> " v)
                  (trace lpf " in query: " code " (" cnt " replacements)")
                  ;;(println lpf "execute replace " pat " --> " v)
                  ;;(println lpf " cleaned: " (escape-replace-str (str v)))

                  (let [res 
                  {:code (str/replace code pat (escape-replace-str (str v)))
                   :replCnt (assoc replCnt k cnt)}]
                    ;;(println "result of replacement: " res)
                    res))
                ;; TODO: replace with one-liner vExcept/throw-except
                (let [msg (str  lpf "missing one or more parameters:"
                                "\n\ttemplate=" code
                                "\n\treplace-key=" k
                                "\n\treplace-val=" v
                                "\n\tother-params=" (dissoc params k))]
                  (error msg)
                  (throw (Exception. msg))))))]
         (let [lpf "(replace-pars): "
               cumm {:code code :replCnt {} }
               cumm (reduce replace-par cumm (seq params))]
;;           (trace lpf "after replacement: " (:code cumm))
           cumm))))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;    Do type conversion on strings according to a type-map  
;;       (special cases such as null, and NaN values handled too)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; TODO:  merge code with vmap/get-map-type-convertor
;;   1. use this code for the convertors and vMap for the structure
;;   2. take the type-cleansing from vMap
;;   3. make separate convertors and use these for the anchor-model extractors too.
(defn convert-type-params 
  "For all parameters defined in the type-map convert their type to the corresponding type.  
   If no type is defined the parameters are assumed to be string-parameters (not checked)."
  [pars typeMap]
  (let [lpf "(convert-type-params): "
        convert-int  (fn [x]
                       (when x
                         (let [x (str/trim x)]
                           (when (not (re-find #"(?i)null|n/a" x))   ;; null will be mapped to a nil-value
                             (Long/parseLong x)))))
        convert-double (fn [x]
                         (when x
                           (let [x (str/trim x)]
                             (when (not (re-find #"(?i)null|n/a" x))   ;; null will be mapped to a nil-value
                               (if (re-find #"(?i)nan" x)
                                 Double/NaN
                                 (Double/parseDouble x))))))
        convert-date  (fn [x]
                         (when x
                           (let [x (str/trim x)]
                             (when (not (re-find #"(?i)null|n/a" x))   ;; null will be mapped to a nil-value
                               (vDate/convert-to-date x)))))
        convert-timestamp  (fn [x]
                             (when x
                               (let [x (str/trim x)]
                                 (when (not (re-find #"(?i)null|n/a" x))   ;; null will be mapped to a nil-value
                                   (vDate/convert-to-timestamp x)))))
        true-values  #{"TRUE" "T" "YES" "Y" "J"}
        false-values #{"FALSE" "F" "NO" "N" "NEE"}
        convert-boolean  (fn [x]
                             (when x
                               (let [x (str/upper-case (str/trim x))]
                                 (if (true-values x)
                                   true
                                   (if (false-values x)
                                     false
                                     (vExcept/throw-except lpf "Boolean should either be " 
                                                           true-values " or " false-values ". Received value: " x))))))
        convert-edn (fn [x]
                      (edn/read-string x))
        convert-type (fn [cumm [k tp]]
                       (if-let [value (get cumm k)]
                         (let [converted (case (keyword tp) 
                                           (:int :integer :long) (convert-int value)
                                           (:double :real (keyword "double precision")) (convert-double value)
                                           :date (convert-date value)
                                           :timestamp (convert-timestamp value)
                                           :boolean   (convert-boolean value)
                                           :edn       (convert-edn value)
                                           (vExcept/throw-except lpf "Unknown type: " tp 
                                                                 " in type-map: " typeMap 
                                                                 " for parameters: " pars))]
                           (assoc cumm k converted))
                         cumm))]
  (reduce convert-type pars typeMap)))


(defn gen-strip-leading
  "This function generates a function that takes a record as input 
   and removes a leading string (possible one-character) from the value under key 'theKey'."
  [theKey stripLeadStr]
  #(let [ct (theKey %)]
     (if (and ct (.startsWith ct stripLeadStr))
       (assoc % theKey (apply str (rest ct))) ;; strip trailing "-"
       %)))



(defn map-nl-to-doubleStr 
  "Map a string in nl-locale to a normal floating point format string (US-format)."
  [d]
  (-> d
    (str/trim)
    (str/replace #"\." "")   ;; haal . voor duizendtallen weg
    (str/replace #"," ".")))  ;; vervang , door .


(defn sort-seq-alphabetical
  "Sort a sequence such that A < a < B < b. Default ordering is A<B<a<b.
   Keywords and strings can be interleaved."
  [sortSeq]
  (->> sortSeq
    (map (fn [x] (let [vx (if (keyword x) (name x) (str x))]
                   [(->> vx
                    (str/upper-case)
                    (#(interleave % vx)) 
                    (apply str)) x])) )
    (#(do (doseq [y %] (println y)) %))
    (sort-by first )
    (map second) ))



