(ns vinzi.tools.inspect)

(refer-clojure)

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
