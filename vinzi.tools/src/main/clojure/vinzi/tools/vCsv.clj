(ns vinzi.tools.vCsv
  (:use [clojure [pprint :only [pprint pp]]]
        [clojure.tools [logging :only [error info trace debug warn]]]
        [vinzi.tools.vSql :only [qs qsp sqs]])
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as sql]
            [clojure
             [string :as str]
             [set :as set]]
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

;; (def readerParams (atom ()))

;; (defn set-reader-params
;;   "Expand the parameters to a sequence and set them as readParams.
;;    Existing params are overwritten."
;;   [pars]
;;   {:pre [(map? pars)]}
;;   (let [seqPars (reduce concat pars)]
;;     (swap! readerParams (fn[_] seqPars))))

(defn extract-readerParams
  "Extract the (file-)readerParams and return a tuple with
   [pars readerPars], where the readerPars are a sequence and
   pars excludes the readerPars."
  [pars]
  (let [{:keys [readerParams]} pars
        pars (dissoc pars :readerParams)
        seqPars (reduce concat readerParams)
        ]
    [pars seqPars]))

(defn extract-readerParams-seq
  [opts]
  (let [kv (partition 2 opts)
        partFunc #(= (first %) :readerParams)
        readerParams (filter partFunc kv )
        opts (reduce concat (remove partFunc kv))]
    [opts (reduce concat readerParams)]))


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
          to-map (fn [row]
                   ;; first the argument to a string
                   (zipmap header (map #(str/trim (str %))row)))
          ;; clojure.java.jdbc uses zipmaps to (see resultset-seq  in file jcdb.clj)
          ;; 2650 ms  so 2,6 ms per row, same timing here
          ]
      (map to-map data))))


;; performance toMap when processing a file containing 1000 data-rows)
;;           to-map (fn [row]  (into {} (map #(vector %1 (str/trim %2)) header row)))]
;;  2650 ms



;; this is a cdp-specific function. cdp provides the extend-path implementation, 
;; but this interface could also be used by others (has wider range of applicability)
(defn extend-csv-path "If a global symbol 'extend-path exists in the namespace of the caller
 this function is applied, otherwise the current directory is prefixed."
  [csvFile]
;;  (println "current namespace = " *ns*)
  (let [lpf "(extend-csv-path): "
        globExtPath (resolve 'extend-path)]
;;    (println "extend-path resolved to : " globExtPath "which is fn? :" (fn? globExtPath))
;;    (if (fn? globExtPath)
    ;; previous line does not work as globExtPath is not recognised as a function.
    (if (and globExtPath (bound? globExtPath))
      (let [extPath (eval (list globExtPath csvFile))]
        (debug lpf "extended path with extend-path function: " globExtPath " resulting in: " extPath)
        extPath)
      (if (vFile/full-path? csvFile)
        (do
          (debug lpf "csv-file is full path already: " csvFile)
          csvFile)
        (let [extPath (vFile/filename (vFile/get-current-dir) csvFile)]
          (debug lpf "extending path with current-dir to: " extPath)
          extPath)))))


  
(defn read-csv-map "Reads a csv dataset from 'fName' using the options 
 and translates it to a clojure map (returned sequence is not lazy).
   Consider using vCsv/read-csv instead as it provides a more extensive range of options."
  [fName & opts]
  {:pre [(string? fName)]}
  (let [lpf "(read-csv-map): "
	[opt readerParams] (extract-readerParams-seq opts)
        fName (extend-csv-path fName)]
    (with-open [f (apply io/reader fName readerParams)]
      (let [csvData (apply csv/read-csv f opts)
            _ (trace lpf "csvData (first 10) = " (apply str (take 10 csvData)))
            csvMap  (csv-to-map csvData)]
        (trace lpf "csvMap (first 10)  = " (with-out-str (pprint (take 10 csvMap))))
        ;; csvMap is non-lazy as file will be closed
        ;; (a template for a lazy-open is given below)
        (doall csvMap)))))



(defn csv-columnMap
  "Apply a column-mapping as defined by 'columnMap' to the dataset. If 'keepAll' is set to false, then
   the columns that do not exist will be dropped from the dataset. 'KeywordizeKeys' turns the keys to 
   keyword (default is using strings).
   A columnMap consist of key-value pairs where the key is the column-name in the csv-file and the value is the name in the target-map.
   (both keywords and strings are allowed) set keepAllColumns to false to limit the number of colums.
  The result is a clean rectangular matrix (list of vectors). Apply csv-to-map to get a sequence of hash-maps.
  "
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
  "Lazy read csv based on params-map. 
   Required key is :csvFile. Allowed keys are :quote :separator :lowCaseKey :columnMap and :keepAllColumns.
   The processFunc is applied to the full-sequence of hash-maps that is produced (followed by doall, so it's not lazy.
   A columnMap consist of key-value pairs where the key is the column-name in the csv-file and the value is the name in the target-map.
   (both keywords and strings are allowed) set keepAllColumns to false to limit the number of colums.
  When a param :checkKeys is provided it will be checked whether each record contains exactly these keys (nil values are allowed). It throws
  an exception on failure."
  [params processFunc]
  (let [lpf (str "(read-csv " params ")")
        [params readerParams] (extract-readerParams params)
        _ (debug lpf "with params: " (with-out-str (pprint params)))
        {:keys [lowCaseKey columnMap keywordizeKeys checkKeys]}  params
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
        (with-open [f (apply io/reader csvFile readerParams)]
;          (let [csvData (apply csv/read-csv f csvOpts)
;                _ (trace lpf "csvData = " (apply str csvData))
;                csvMap  (csv-to-map csvData lowCaseKey)]
          (let [showFirstThree (fn [recs msg] 
                                 (debug msg "(first 3): " (with-out-str (pprint (take 3 recs)))) 
                                 recs)
                check-keys (if checkKeys
                             (let [cks (set checkKeys)
                                   lpf "(read-csv-lazy$check-keys): "]
                               (debug lpf "Define check-keys function for: " checkKeys)
                               (fn [x] (map #(let [ks (set (keys %))]
                                               (when (not= cks ks)
                                                   (let [extra (set/difference ks cks)
                                                         missing (set/difference cks ks)]
                                                     (vExcept/throw-except lpf "Incorrect keys in Record: " 
                                                                           (when (seq missing) (str "\n\tmissing keys " (str/join "," missing)))
                                                                           (when (seq extra) (str "\n\tredundant keys " (str/join "," extra)))
                                                          "\n\t rec=" %
                                                          "\n\tRecord does NOT contain the correct keys.")
                                                     )) %) 
                                            x)))
                               identity)  ;; nothing to match against
                csvMap (-> (apply csv/read-csv f csvOpts)
                         (showFirstThree "read-csv")
                         (csv-columnMap columnMap keepAllColumns keywordizeKeys)
                         (showFirstThree "csv-columnMap")
                         (csv-to-map lowCaseKey)
                         (showFirstThree "csv-to-map")
                         (check-keys))]
            ;; csvMap is lazy but terminates when the scope of this (with-open is closed.
            ;;    (a template for a lazy-open that keeps a file open is given below)
            ;; Sept 2012, added a doall to enforce realization (lazyness should be within processfunc
            (doall (processFunc csvMap)))))
      (throw (Exception. (str lpf "No parameter :csvFile found in call to read-csv"))))))

(defn read-csv
  "Non-lazy read csv based on params-map.
   Required key is :csvFile. Allowed keys are :quote :separator :lowCaseKey :columnMap and :keepAllColumns.
   The processFunc is applied to the full-sequence of hash-maps that is produced (followed by doall, so it's not lazy).
   A columnMap consist of key-value pairs where the key is the column-name in the csv-file and the value is the name in the target-map.
   (both keywords and strings are allowed) set keepAllColumns to false to limit the number of colums.
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
              mapConv (vMap/get-map-type-convertor colInfo)
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
  "Map a sequence of maps to a format that can be output to a csv (first row contains column-names, subsequent rows the data).
   If a second parameter is provided it will be uses as the sequence of keys to be included.
  Koloms are ordered lexicographical."
  ([data]
    (map-seq-to-csv data nil))
  ([data k & opts]
  (let [_ (println " received opt-seq: " opts "  of type "  (type opts))
        opts (apply hash-map opts)
        _ (println " passed options are: " opts)
        opts (into  {:sortHeader true} opts)
        _ (println " options with defaults are: " opts)
        ;; when not provided take the keys of the first records
        k (if k k (keys (first data)))
        ;; prepare ordered keys (but retain type (keyword, string)
        ;; j
         _ (println "k: "  k)       
        k (if (:sortHeader opts)
            (->> k
              (map #(vector (if (keyword %) (name %) %) %))
              (sort-by first)
              (map second))
            k)
        _ (println "opts are: " opts)
        _ (println "key-seq: "  k)
        vecData (map #(vec (map % k)) data)
        ks (vec (map name k))]
    (cons ks vecData))))


(comment ;; test example map-seq-to-csv
  (def recs  '({:a 1 :b 2} {:a 3 :b \c}))
  (map-seq-to-csv recs)
  ;; output  (first line are headers
  ;; (["a" "b"] [1 2] [3 \c])
  )

(defn check-data-type 
  "Perform check on type of data whether it is sequential, or can be treated as a sequential."
  [fName data]
  (when (not (or (sequential? data)
                 (= (type data) clojure.lang.PersistentHashSet)
                 (associative? data)))   ;; hashmap
    (error "(vCsv/check-data-type): data for file " fName 
                          ". Expected sequential as data. Received data of type: " (type data))
    (error "first data element: "(with-out-str (pprint (first data))))
    (vExcept/throw-except "(vCsv/check-data-type): received invalid data of type: " (type data))))


(defn write-csv-stream 
  "Write a sequence of maps (as produced by vCsv/read-csv-map) to an open steam in csv-file format.
   The keys of the first map-item will be used a keys for the full sequence."
  [stream data & opts]
  (check-data-type "STREAMING" data)
  (let [data (map-seq-to-csv data)]
    (apply csv/write-csv stream data opts)))

(defn write-csv 
  "Write a sequence of maps (as produced by vCsv/read-csv-map) to a csv-file again.
   The keys of the first map-item will be used a keys for the full sequence."
  [fName data & opts]
  (check-data-type fName data)
  ;; some checks on the validity of the options.
  (let [lpf "(vCsv/write-csv): "
        csvOpts (apply hash-map opts)
        newlineOpts #{nil :lf :cr+lf}]
    (if-let [unknown (seq (remove #(#{:separator :quote :quote? :newline} (first %)) csvOpts))]
      (vExcept/throw-except lpf " Unknown option(s): " unknown) 
      (when-not (or (nil? (:newline csvOpts)) (newlineOpts (:newline csvOpts)))  ;; nil requires separate test
        (vExcept/throw-except lpf " Only valid values for options :newline are : " newlineOpts " however received: " (:newline csvOpts)) )))
  ;; the core code
  (with-open [out (io/writer fName)]
    ;; TODO: use write-csv-stream instead of code below
    (let [data (map-seq-to-csv data)]
        (apply csv/write-csv out data opts))))


(defn project-fill 
  "Similar to set/project, however, missing values are patched with nil.
   (Use this instead of set/project when preparing a collection to write a csv, to prevent missing keys)"
  [xrel ks]
  {:pre [(sequential? xrel)]}
  (map #(zipmap ks (map % ks)) xrel))




(defn parse-csv-str 
  "Cleanse the string by removing trailing white-spaces per line and trailing \newline (at end of file).
   Next the string is parsed as .csv"
  [data]
  (let [lpf "(parse-csv-str): "]
  (-> data
    (str/replace #"([^\"])\"[\s]+\n" "$1\"\n")  ;; strip white-space between non-escaped \" and end-of-line.
    (str/replace #"\n*$" "") ;; strip all trailing newlines
    (str/replace #"$" "\n")
    (java.io.StringReader. )
    (csv/read-csv  :separator \;)
    (#(do (debug lpf "after read-csv: " 
                 (def ^:dynamic debLast %) (with-out-str (pprint %))) %))
    (csv-to-map )
    (doall))))   ;; NOTE: this doall is essential to prevent the java.io.StringReader is garbage-collected to early
    ;; TODO: check whether doall is realy needed


;; Template for a lazy-open-file that closes the file after reading the
;; last line



(defn pprint-csv [params numLines]
  (let [make-sorted (fn [m]
                      (apply sorted-map (interleave (keys m) (vals m))))
        process-lines (fn [recs]
                        (doseq [r (take numLines recs)]
                          (pprint (make-sorted r))))]
  (read-csv-lazy params process-lines)))



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


