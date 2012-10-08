(ns vinzi.tools.vCsv
  (:use [clojure [pprint :only [pprint pp]]]
        [clojure.tools [logging :only [error info trace debug warn]]]
        [vinzi.tools.vSql :only [qs qsp]])
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as sql]
            [clojure.string :as str]
            [vinzi.tools
             [vFile :as vFile]
             [vMap :as vMap]
             [vSql :as vSql]]
            ))

(defn csv-to-map "Takes a csv-dataset (sequence of vectors) 
 and translates it to a (lazy) sequence of maps, using the first row as map-keys.
 Surrounding spaces are trimmed, and all values are returned as strings."
  [csv]
  (let [lpf "(csv-to-map): "
        header (->> (first csv)
                 (map str/trim)  ;; do not allow surrounding spaces on keyword
                 (map keyword))
        _ (debug lpf "with headers: " (str/join ", " header))
        data   (next csv)
        to-map (fn [row]
                 (into {} (map #(vector %1 (str/trim %2)) header row)))]
    (map to-map data)))


;; this is a cdp-specific function. cdp provides the extend-path stuff, but this is wider applicable
(defn extend-csv-path "If a global symbol 'extend-path exists in the namespace of the caller
 this function is applied, otherwise the current directory is prefixed."
  [csvFile]
;;  (println "current namespace = " *ns*)
  (let [globExtPath (resolve 'extend-path)]
;;    (println "extend-path resolved to : " globExtPath "which is fn? :" (fn? globExtPath))
;;    (if (fn? globExtPath)
    ;; previous line does not work as globExtPath is not recognised as a function.
    (if (and globExtPath (bound? globExtPath))
      (eval (list globExtPath csvFile))
      (if (vFile/full-path? csvFile)
        csvFile
        (vFile/filename (vFile/get-current-dir) csvFile)))))


  
(defn read-csv-map "Reads a csv dataset from 'fName' using the options 
 and translates it to a clojure map (returned sequence is not lazy)."
  [fName & opts]
  {:pre [(string? fName)]}
  (let [lpf "(vinzi.cdp.tools.csv/read-csv-map): "
        fName (extend-csv-path fName)]
    (with-open [f (io/reader fName)]
      (let [csvData (apply csv/read-csv f opts)
            _ (trace lpf "csvData = " (apply str csvData))
            csvMap  (csv-to-map csvData)]
        (trace lpf "csvMap = " (with-out-str (pprint csvMap)))
        ;; csvMap is non-lazy as file will be closed
        ;; (a template for a lazy-open is given below)
        (doall csvMap)))))



;; actually this is a cdp specific use of params
(defn read-csv-lazy 
  "Lazy read csv based on params-map. Required key is :csvFile. Allowed keys are :quote and :separator."
  [params processFunc]
  (let [lpf (str "(read-csv " params ")")
        ;; translate the two csv-opts from string to character (if needed)
        make-char (fn [p k]
                     (if-let [v (k p)]
                       (if (string? v)
                         (assoc p k (first v))
                         p)
                       p))
        params (reduce make-char params '(:quote :separator))]
    (if-let [csvFile (:csvFile params)]
      (let [csvOpts (apply concat (remove #(nil? (second %)) (map #(vector % (% params)) [:quote :separator])))
            csvFile (extend-csv-path csvFile)]
        (with-open [f (io/reader csvFile)]
          (let [csvData (apply csv/read-csv f csvOpts)
                _ (trace lpf "csvData = " (apply str csvData))
                csvMap  (csv-to-map csvData)]
            (trace lpf "csvMap = " (with-out-str (pprint (first csvMap))))
            ;; csvMap is lazy but terminates when the scope of this (with-open is closed.
            ;;    (a template for a lazy-open that keeps a file open is given below)
            ;; Sept 2012, added a doall to enforce realization (lazyness should be within processfunc
            (doall (processFunc csvMap)))))
      (throw (Exception. (str lpf "No parameter :csvFile found in call to read-csv"))))))

(defn read-csv
  "Non-lazy read csv based on params-map. Required key is :csvFile. Allowed keys are :quote and :separator."
  [params]
  {:pre [(map? params)]}
  (read-csv-lazy params doall))


(def non-lazy false)

(defn read-csv-to-db 
  "Read the data from a csv-file with (read-csv params) and append this data to table :targetTbl 
   while matching the column-names as specified in the csv-file.
   The targetTbl is assumed to be a correctly quoted and sufficiently specified table identifier.
   (Currently does not use naming strategy, so field-names should be in lower-case and should not require quoting.)"
  [params]
  {:pre [(map? params)]}
  ;; no unit-test included yet, as this requires setting up a database connection.
    (let [lpf "(read-csv-to-db): "]
    (if-let [targetTbl (:targetTbl params)]
      (let [{:keys [schema table]} (if-let [schema (:schema params)]
                                     {:schema schema :table targetTbl}
                                     (if (= (first targetTbl) \")
                                       (vSql/split-qualified-name targetTbl)
                                       {:table targetTbl}))
            targetTbl (if schema 
                        (qsp schema table) 
                        (qs table))
            _ (debug lpf "Reading csvFile " (:csvFile params) 
                     " to table " targetTbl 
                     " with opts: " (dissoc params :schema :csvFile :targetTbl)) 
            colInfo (let [schema (if schema schema "public")]
                      (if-let [colInfo (seq (vSql/get-col-info schema table))]
                        colInfo
                        (let [msg (str "No column-information found for " schema "." table)]
                          (error lpf msg)
                          (throw (Exception. msg)))))
            colInfo (zipmap (map #(keyword (:column_name %)) colInfo)
                            (map :data_type colInfo))
            _  (info lpf " The column-information is: " colInfo)
            mapConv (vMap/get-map-str-convertor colInfo)
            processFunc (fn [data]   ;; will be called within scope of read-csv-lazy
                          (if (seq data)
                            (let [firstRec (first data)
                                  noTarget (some #(nil? (colInfo  %)) (keys firstRec))
                                  noSrc    (filter #(nil? (% (set (keys firstRec)))) (keys colInfo)) 
                                  msg (str/join "\n\t"
                                         (map #(str (first %) "=" (second %) " \t--> " (colInfo  (first %))) firstRec))]
                              (when (seq noSrc)
                                (info lpf "\nSome field of the target table do not get a value (continue loading):\n\t"
                                      (str/join "\n\t" noSrc)))
                              (if noTarget
                                (let [msg (str lpf "One or more fields can not be mapped to the database-table:\n\t" msg)]
                                  (error msg)
                                  (throw (Exception. msg)))
                                (info lpf "Implied conversion on first record:\n\t" msg))
                              (apply sql/insert-records targetTbl (map mapConv data)))
                            (warn lpf "No data received.")))]
        (if non-lazy
          (let [_ (trace "starting reading the csv-file")
                data (read-csv (dissoc params :targetTbl :schema))
                _ (trace "starting to convert the data")
                data (map mapConv data)]
            (apply sql/insert-records targetTbl data))
          (read-csv-lazy (dissoc params :targetTbl :schema) processFunc)))
      (throw (Exception. (str lpf "No parameter :targetTbl found in call to read-csv-to-db"))))))


;; Template for a lazy-open-file that closes the file after reading the
;; last line

;; (def lazy-open
;;   (letfn [(read-line [rdr]
;;                      (lazy-seq
;;                       (if-let [line (.readLine rdr)]
;;                         (cons line (read-line rdr))
;;                         (do (.close rdr)
;;                             nil))))]
;;     (fn [file]
;;       (lazy-seq (read-line (clojure.java.io/reader file))))))



(comment ;; example
  (def x (with-open [f (io/reader "/tmp/test.csv")]
         (doall (csv/read-csv f :separator \;))))

  (csv-to-map x)
)


