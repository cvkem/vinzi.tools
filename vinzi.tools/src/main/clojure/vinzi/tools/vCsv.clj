(ns vinzi.tools.vCsv
  (:use [clojure [pprint :only [pprint pp]]]
        [clojure.tools [logging :only [error info trace debug warn]]]
        [vinzi.tools.vSql :only [qs qsp sqs]])
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as sql]
            [clojure.string :as str]
            [vinzi.tools
             [vFile :as vFile]
             [vMap :as vMap]
             [vSql :as vSql]
             [vExcept :as vExcept]]
            ))

;; function use to map the headerline to the keywords.
;(def ^:dynamic KeyWordFunc (fn [x] (-> x (str/trim) (keyword))))
;
;(defn set-KeyWordFunc [kwf]
;  (def KeyWordFunc kwf))

(defn xml-true [x]
;  (if (= (class x) java.lang.Boolean)
;    x
    (if (string? x)
      (= (str/lower-case x) "true")  ;; this is the special case
      x))  ;; boolean, nil  (and other values) treated as expected


(defn csv-to-map "Takes a csv-dataset (sequence of vectors) 
 and translates it to a (lazy) sequence of maps, using the first row as map-keys.
 Surrounding spaces are trimmed, and all values are returned as strings."
  ([csv] (csv-to-map csv false))
  ([csv lowCaseKey]
    (let [lpf "(csv-to-map): "
          header (->> (first csv)
                     (map name)
                     (map str/trim)
                     (map #(if lowCaseKey (str/lower-case %) %))
                     (map keyword))
          _ (debug lpf "with headers: " (str/join ", " header))
          data   (next csv)
          ;; speedup loop below by using a transient data-structure
;; performance toMap when processing a file containing 1000 data-rows)
;;          to-map (fn [row]
;;                   (into {} (map #(vector %1 (str/trim %2)) header row)))]
          ;; 2650 ms  so 2,6 ms per row
          to-map (fn [row]
                   (zipmap header (map str/trim row)))
          ;; clojure.java.jdbc uses zipmaps to (see resultset-seq  in file jcdb.clj)
          ;; 2650 ms  so 2,6 ms per row
          to-map (fn [row]
                   (apply hash-map (interleave header (map str/trim row))))
          ;; 2610 ms  so 2,6 ms per row
          ]
      (map to-map data))))


;; performance toMap when processing a file containing 1000 data-rows)
;;           to-map (fn [row]  (into {} (map #(vector %1 (str/trim %2)) header row)))]
;;  2650 ms



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
  (let [lpf "(read-csv-map): "
        fName (extend-csv-path fName)]
    (with-open [f (io/reader fName)]
      (let [csvData (apply csv/read-csv f opts)
            _ (trace lpf "csvData (first 10) = " (apply str (take 10 csvData)))
            csvMap  (csv-to-map csvData)]
        (trace lpf "csvMap (first 10)  = " (with-out-str (pprint (take 10 csvMap))))
        ;; csvMap is non-lazy as file will be closed
        ;; (a template for a lazy-open is given below)
        (doall csvMap)))))

(comment ;; test performance
;; read-csv-map seems to be slow. Timing on 114800 rows:
;;
(def csvFile "resources/scipio/atcMapping.csv")
;;
(time (def bareRows 
                        (with-open [inp (io/reader csvFile)]
                          (doall (csv/read-csv inp :separator \;)))))
;;"Elapsed time: 236.999671 msecs"
;;  or 1e-6 per row
;;
;; 
(time (def rows (vCsv/read-csv-map csvFile :separator \;)))
;;"Elapsed time: 3000 msecs"
;;  0.03 second per row

;; when adding the  (doall at the line 'csvData (doall (apply csv/read-csv f opts))'
;; the timing reduces in an extreme way to
;; "Elapsed time: 3170.485984 msecs"
;;  or 0.03 per row


)


(defn csv-columnMap
  "Apply a column-mapping as defined by 'columnMap' to the dataset. If 'keepAll' is set to false, then
   the columns that do not exist will be dropped from the dataset. 'KeywordizeKeys' turns the keys to 
   keyword (default is using strings)."
  ([data columnMap] (csv-columnMap data columnMap true false))
;;  option with three parameters NOT included to prevent errors on ordering of the boolean flags.
;;  (specify both or none).  
  ([data columnMap keepAll keywordizeKey]
    (let [lpf "(csv-columnMap): "]
      (if columnMap
        (if (map? columnMap)
          (let [cols (->> (first data)
                       (map #(if-let [newName (or (columnMap %) (columnMap (keyword %)))]
                               newName
                               (when keepAll
                                 %)))
                       (map #(when % (name %))))
                colNrs (->> cols
                         (map #(list %1 %2) (range))
                         (remove #(nil? (second %)))
                       (map first))
                ;;colLabels (vec (remove nil? cols))
                colLabels (->> cols
                            (remove nil?)
                            (map #(if keywordizeKey (keyword %) %))
                            (vec))
                newData (map (fn [row] (vec (map #(get row %) colNrs))) (rest data))]
            (debug lpf "colLabels: " colLabels " taken from columns: " (str/join "," colNrs))
            (concat (list colLabels) newData))
          (vExcept/throw-except "columnMap should be map. Received value of type: " (type columnMap)))
        data))))




;; actually this is a cdp specific use of params
(defn read-csv-lazy 
  "Lazy read csv based on params-map. Required key is :csvFile. Allowed keys are :quote :separator :lowCaseKey :columnMap and :keepAllColumns.
   The processFunc is applied to the full-sequence of hash-maps that is produced (followed by doall, so it's not lazy."
  [params processFunc]
  (let [lpf (str "(read-csv " params ")")
        _ (debug lpf "with params: " (with-out-str (pprint params)))
        {:keys [lowCaseKey columnMap keywordizeKeys]}  params
        keepAllColumns (if (some #{:keepAllColumns} (keys params))
                         (:keepAllColumns params)
                         true)   ;; if key does not exist default to true
        lowCaseKey (xml-true lowCaseKey)
        params     (dissoc params :lowCaseKey :columnMap :keepAllColumns :keywordizeKeys)
        make-char (fn [p k]
                     (if-let [v (k p)]
                       (if (string? v)
                         (assoc p k (first v))
                         p)
                       p))
        ;; translate the two csv-opts from string to character (if needed)
        params (reduce make-char params '(:quote :separator))]
    (if-let [csvFile (:csvFile params)]
      (let [csvOpts (apply concat (remove #(nil? (second %)) (map #(vector % (% params)) [:quote :separator])))
            csvFile (extend-csv-path csvFile)]
        (with-open [f (io/reader csvFile)]
;          (let [csvData (apply csv/read-csv f csvOpts)
;                _ (trace lpf "csvData = " (apply str csvData))
;                csvMap  (csv-to-map csvData lowCaseKey)]
          (let [showFirstThree (fn [recs msg] 
                                 (debug msg "(first 3): " (with-out-str (pprint (take 3 recs)))) 
                                 recs)
                csvMap (-> (apply csv/read-csv f csvOpts)
                         (showFirstThree "read-csv")
                         (csv-columnMap columnMap keepAllColumns keywordizeKeys)
                         (showFirstThree "csv-columnMap")
                         (csv-to-map lowCaseKey)
                         (showFirstThree "csv-to-map"))]
            ;; csvMap is lazy but terminates when the scope of this (with-open is closed.
            ;;    (a template for a lazy-open that keeps a file open is given below)
            ;; Sept 2012, added a doall to enforce realization (lazyness should be within processfunc
            (doall (processFunc csvMap)))))
      (throw (Exception. (str lpf "No parameter :csvFile found in call to read-csv"))))))

(defn read-csv
  "Non-lazy read csv based on params-map. Required key is :csvFile. Allowed keys are :quote and :separator.
   (Returns a non-lazy sequence of hash-maps)."
  [params]
  {:pre [(map? params)]}
  (read-csv-lazy params doall))


(def non-lazy false)

(defn read-csv-to-db 
  "Read the data from a csv-file with (read-csv params) and append this data to the existing table :targetTbl 
   while matching the column-names as specified in the csv-file. 
   Params is a map with required keys :csvFile :targetTbl and optional keys :schema :separator :quote :lowCaseKey :columnMap :keepAllColumns
  ?? The targetTbl is assumed to be a correctly quoted and sufficiently specified table identifier.
   (Currently does not use naming strategy, so field-names should be in lower-case and should not require quoting.)"
  [params]
  {:pre [(map? params)]}
  ;; no unit-test included yet, as this requires setting up a database connection.
    (let [lpf "(read-csv-to-db): "
          ;;  keepAllColumns also has a meaning to csv-columnMap (when combined with columnMap)  !!
          keepAllColumns (if (some #{:keepAllColumns} (keys params))
                           (:keepAllColumns params) true) 
          params (assoc params :keywordizeKeys true)]
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
              _  (info lpf " The column-information is (TEMP): " colInfo)
              mapConv (vMap/get-map-str-convertor colInfo)
              processFunc (fn [data]   ;; will be called within scope of read-csv-lazy
                            (if (seq data)
                              (let [firstRec (first data)
                                    noTarget (some #(nil? (colInfo  %)) (keys firstRec))
                                    noSrc    (filter #(nil? (% (set (keys firstRec)))) (keys colInfo)) 
                                    msg (str/join "\n\t"
                                         (map #(str (first %) " = " (second %) " \t--> " (colInfo  (first %))) firstRec))]
                                (when (seq noSrc)
                                  (info lpf "\nSome field(s) of the target table do not get a value (continue loading):\n\t"
                                        (str/join "\n\t" noSrc)))
                              (if (and keepAllColumns noTarget)
                                (throw (Exception. (str lpf "One or more fields can not be mapped to the database-table:\n\t"
                                                        "fieldname = value  -->  target-type \n\t"
                                                        msg)))
                                (info lpf "Implied conversion on first record:\n\t" msg))
                              (if (and (some #{:lowCaseKey} (keys params))    ;; the key exists 
                                       (not (xml-true (:lowCaseKey params))))            ;; and its value is false 
                                (do   ;; vSql/with-db-caps
                                  (debug lpf "DB-handling in a case-sensitive way. First record: " (first data))
                                  (let [cols    (keys (first data))
                                        qryFmt (str "INSERT INTO " (qs targetTbl)
                                                    " (" (str/join "," (map #(qs (name %)) cols)) ") "
                                                    " VALUES (%s);")
                                        showCnt (atom 0) ]
                                    ;; TODO: investigated whether this could be prepared statement
                                    ;; or we can get the naming strategy for uppercase working for insert-records
                                    (debug lpf "format is: " qryFmt)
                                    ;; TODO: may-be mapConv should remove empty lines, or check number of data-items
                                    ;;  before running mapCon (empty last line now kills the data-load)
                                    (doseq [d (map mapConv data)]
                                      (let [values (->> cols
                                                     (map #(% d))
                                                     (map #(if (string? %) (sqs %) (str %)))
                                                     (str/join ","))
                                            qry (format qryFmt values)]
                                        (when (<= @showCnt 10000)
                                          (swap! showCnt inc)
                                          (debug lpf "insert stmt " @showCnt ": " qry))
                                        (sql/do-commands qry)))))
                                (let [upKeys (remove nil? (map #(let [k (name %)] (when (not= k (str/lower-case k)) k)) (keys colInfo)))] 
                                  (debug lpf "DB-handling with lower-case fieldnames. First record: " (first data))
                                  (when (seq upKeys)
                                    (error lpf "Some of the keys are in upper-case: " (str/join ", " upKeys)))
                                  (apply sql/insert-records targetTbl (map mapConv data)))))
                              (warn lpf "No data received.")))]
        (debug lpf "Reading csv-file with params: "  params)
        (if non-lazy
          (let [data (read-csv (dissoc params :targetTbl :schema))
                _ (trace "starting to convert the data")
                data (map mapConv data)]
            (apply sql/insert-records targetTbl data))
          (read-csv-lazy (dissoc params :targetTbl :schema) processFunc)))
      (throw (Exception. (str lpf "No parameter :targetTbl found in call to read-csv-to-db"))))))



(defn map-seq-to-csv 
  "Map a sequence of maps to a format that can be output to a csv."
  [data]
  (let [k (keys (first data))
        vecData (map #(vec (map % k)) data)
        ks (vec (map name k))]
    (cons ks vecData)))


(comment ;; test example map-seq-to-csv
  (def recs  '({:a 1 :b 2} {:a 3 :b \c}))
  (map-seq-to-csv recs)
  ;; output  (first line are headers
  ;; (["a" "b"] [1 2] [3 \c])
  )

(defn write-csv 
  "Write a sequence of maps (as produced by vCsv/read-csv-map) to a csv-file again.
   The keys of the first map-item will be used a keys for the full sequence."
  [fName data & opts]
  (with-open [out (io/writer fName)]
    (let [data (map-seq-to-csv data)]
        (apply csv/write-csv out data opts))))




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


(defn pprint-csv [params numLines]
  (let [make-sorted (fn [m]
                      (apply sorted-map (interleave (keys m) (vals m))))
        process-lines (fn [recs]
                        (doseq [r (take numLines recs)]
                          (pprint (make-sorted r))))]
  (read-csv-lazy params process-lines)))

(comment ;; example
  (def x (with-open [f (io/reader "/tmp/test.csv")]
         (doall (csv/read-csv f :separator \;))))

  (csv-to-map x)
)


