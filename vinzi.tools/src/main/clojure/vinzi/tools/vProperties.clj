(ns vinzi.tools.vProperties
;  (:use	[clojure [pprint :only [pprint pp]]]
;;        [clojure [stacktrace :only [print-stack-trace root-cause]]]
;;        [clojure.tools [logging :only [error info trace debug warn]]])
  (:use       [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [string :as str]]
            [vinzi.tools [vFile :as vFile]])
  (:import [java.io     File   BufferedReader FileInputStream FileOutputStream BufferedInputStream]
           [java.util   Properties]))


(defn filter-prefix  
  "Filter the properties having prefix, and
   return the filtered properties without this prefix."
  [props prefix]
  (if prefix 
    (let [lpf "(filter-prefix): "
          prefix (if (= (last prefix) \.) prefix (str prefix "."))
          lp (count prefix)]
      (debug lpf (str "limit properties to prefix: '" prefix "'."))
      (->> props 
        ;; a single reduce is more efficient.
        (filter #(.startsWith (name (first %)) prefix) )
        (map #(vector (apply str (drop lp (name (first %)))) (second %)) )
        (into {} )))
    props))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;    Look at vinzi.tools.vParams for a more high-level interface with utilities 
;;       - automatic type conversion
;;       - command-line overrides, 
;;       - etc..
;;
;;    reading and writing properties
;;     based on clojure.contrib.java-utils  (clojure 1.2)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn as-prop-str
  "Like clojure.core/str, but if an argument is a keyword or symbol,
its name will be used instead of its literal representation.

Example:
 (str :foo :bar) ;;=> \":foo:bar\"
 (as-prop-str :foo :bar) ;;=> \"foobar\"

Note that this does not apply to keywords or symbols nested within
data structures; they will be rendered as with str.

Example:
 (str {:foo :bar}) ;;=> \"{:foo :bar}\"
 (as-prop-str {:foo :bar}) ;;=> \"{:foo :bar}\" "
  ([] "")
  ([x] (if (instance? clojure.lang.Named x)
         (name x)
         ;; extension by cvk (escape the : in properties
         ;;(str/replace (str x) ":" "\\:")
         ;; (niet nodig, wordt al gedaan door java property-writer
         (str x)
         ))
  ([x & ys]
    ((fn [#^StringBuilder sb more]
       (if more
         (recur (. sb (append (as-prop-str (first more)))) (next more))
         (str sb)))
      (new StringBuilder #^String (as-prop-str x)) ys)))




; Not there is no corresponding props->map. Just destructure!
(defn #^Properties extend-properties
  "Extend a java.utils.Properties instance with the mapping pairs provided by m.
Uses as-prop-str to convert both keys and values into strings."
  {:tag Properties}
  [p m]
  ;; p is a mutable java.properties object!!
    (doseq [[k v] m]
      (.setProperty p (as-prop-str k) (as-prop-str v)))
  p)

; Not there is no corresponding props->map. Just destructure!
(defn #^Properties as-properties
  "Convert any seq of pairs to a java.utils.Properties instance.
Uses as-prop-str to convert both keys and values into strings."
  {:tag Properties}
  [m]
  (let [p (Properties.)]
    (extend-properties p m)))


(defn read-properties
  "Read properties from file-able, translate the string-keys to keywords and set defaults.
   Fails with exception if the file does not exist unless you pass in a default hash-map."
  ([fName]
    (with-open [f (java.io.FileInputStream. fName)]
      (let [props (doto (Properties.)
                    (.load f))
            to-hashmap #(zipmap (map keyword (keys %1)) (vals %1))]
  (to-hashmap props))))  ;; translate java.util.properties to a normal hash-map (otherwise assoc does not work)
  ;; also translating string-keys to keywords
  ([fName default]
    {:pre [(map? default)]}
    (if (vFile/file-exists fName)
      (into default (read-properties fName))
      default)))


(defn write-properties
  "Write properties to file-able."
  {:tag Properties}
  ([m fName] (write-properties m fName nil))
  ([m fName comments]
    (with-open [#^FileOutputStream f (FileOutputStream. fName)]
      (doto (as-properties m)
        (.store f #^String comments)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;   end of properties (based on clojure.contrib.java-utils) ;;;;;;;;;;;;;;;;;;
