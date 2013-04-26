(ns vinzi.tools.vParams
  (:use	[clojure 
         [pprint :only [pprint pp]]]
        [clojure 
         [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools 
         [logging :only [error info trace debug warn]]])
  (:require [clojure.string :as str]
            [vinzi.tools 
             [vExcept :as vExcept]
             [vProperties :as vProp]
             [vMap :as vMap]]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Handling commandline parameters and options.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn get-param "Extract a parameter staring with 'label=' from the
  argument list."
  ([args label] (get-param label args nil))
  ([args label default]
    (if (nil? args)
      nil
      (do
        ;; check parameters
        (when (not (sequential? args))
          (vExcept/throw-except "Arguments should be a sequential, received a type: " (type args) " with value: " args))
        (when (not (string? label)) 
          (vExcept/throw-except "Label should be a string, received a type: " (type label) " with value: " label))
        (when (not (or (nil? default) (string? default)))
          (vExcept/throw-except "Label should be a string or nil, received a type: " (type default) " with value: " default))
        
        (let [label (str/trim label)
              lbl   (str (str/lower-case label) "=")
              pars (seq (filter #(.startsWith (str/lower-case %) lbl) args))]
          (when (> (count pars) 1)
            (error "Received multiple values for parameter " label ": " pars
               "\n\tOnly handling first value."))
          (if (seq pars)
            (let [value (apply str (drop (count lbl) (first pars)))]
              (info "for label: " label " extracted value=" value)
              value)
            default))))))

(defn get-opt "Extract a parameter starting with 'label=' from the
  argument list (returns the actual values obtained or nil)."
  [args label]
    {:pre [(sequential? args) (string? label)]}
  (let [lbl (str/lower-case (str/trim label))]
    (seq (filter #(= lbl (str/lower-case %)) args))))


;; TODO: command-line overrides args   which overrides all props and which issues a warning/error if an option does not exist.
;;   (possibly ignoring options without an "=" sign)

(defn commandline-override 
  "Check if parameter (name k) exists in array of string args assuming each string represents a key=value.
   If the key corresponding to (keyword k) exists in props.
    The value is stored with the same type as the current value under that key in props.
    NOTE/TODO: date-transformation is not supported yet."
  [args k props]
  ;;(println "called commmandline-override with: args=" args " k=" k " and props=" props)
  (if-let [v (get-param args (name k) nil)]
    (let [kw (keyword k)
          ;;_  (println " type of original: " (type (kw props)))
          tp (type (kw props))
          v (cond 
              (= tp clojure.lang.Keyword) (keyword v)
              (= tp java.lang.Boolean) (java.lang.Boolean/parseBoolean v)
              (= tp java.lang.Long)    (java.lang.Long/parseLong v)
              (= tp java.lang.Integer) (java.lang.Integer/parseInt v)
              (= tp java.lang.Double)  (java.lang.Double/parseDouble v)
              :else v)]
    (assoc props kw v))
    props))

(defn commandline-override-all 
  "Check if any of the parameters out of props in string args a key=value and if override it."
  [args props]
  ;;(println "keys of props are: " (keys props) " and props = " props)
    (reduce #(commandline-override args %2 %1) props (keys props)))


(defn show [msg r]
  (do (debug msg (with-out-str (pprint r))) r))

(defn get-properties 
  "Get the properites of a file, keywordize them and transform them according to typeMap."
  [propFile defMap typeMap]
  (-> propFile
    (vProp/read-properties)
    ;;((partial show " props="))
    (vMap/keywordize false)
    ;;((partial show " keywordized="))
    ((partial into defMap ))
    ;;((partial show " with defaults="))
    ((vMap/get-map-type-convertor typeMap) )
    ((partial show " after type-conversion="))
    ))


