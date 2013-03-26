(ns vinzi.tools.inspect
  (:require [clojure 
             [string :as str]
             [reflect :as refl]])
 ;; (:refer-clojure)
  )

;;(refer-clojure)

(defn find-symbol-in-ns [ns regExp]
  (filter #(re-find regExp (str (second %))) (ns-map (if (string? ns) (symbol ns) ns))))


(defn show-methods
  "Retrieve all methods of (the class of) a java-object."
  ([obj]  (show-methods obj ""))
  ([obj mask]
     (for [method (seq (.getMethods (class obj)))
	   :let [method-name (.getName method)]
	   :when (re-find (re-pattern mask) method-name)]
       method-name)))

(defn show-fields
  "Retrieve all fields of a (the class of a) java object."
  ([obj] (show-fields obj ""))
  ([obj mask]
     (for [field (seq (.getFields (class obj)))
	   :let [field-name (.getName field)]
	   :when (re-find (re-pattern mask) field-name)]
       field-name)))

(defmacro getter
  "POC that shows how to turn a string into a field reference."
  [obj field]
  (let [cls (class obj)
	fieldName (str cls "/" field)
	_ (println " the fieldame = " fieldName)
	fieldSymbol (symbol fieldName)]
    `(. ~obj ~fieldSymbol)))



(defn get-members 
  "Inspect an object and print formated output"
  [x] 
     (let [pt (fn [x] (case (str (class x))
                        "class clojure.reflect.Method"  "METHOD"
                        "class clojure.reflect.Field"   "FIELD"
                        "class clojure.reflect.Constructor" "CONSTR"
                        (type x)))
           pa (fn [x] (case (str (class x))
                        "class clojure.reflect.Method"  (str (:parameter-types x))
                        "class clojure.reflect.Field"   ""
                        "class clojure.reflect.Constructor" ""
                        (type x)))]
     (println "Object of type: " (type x)  " has members: ")
     (print (str/join "\n" (map #(str (pt %) "\t " (:name %) "\t" (pa %) ) (:members (refl/reflect x)))))))

