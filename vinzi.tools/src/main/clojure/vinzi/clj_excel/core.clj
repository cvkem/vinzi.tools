(ns vinzi.clj-excel.core
  (:use clojure.java.io
        clojure.tools.logging)
  (:import [org.apache.poi.xssf.usermodel XSSFWorkbook]
           [org.apache.poi.hssf.usermodel HSSFWorkbook]
           [org.apache.poi.ss.usermodel Row Cell DateUtil WorkbookFactory CellStyle Font]))


;;;;
;;;   Based on (copy of)    https://github.com/mebaran/clj-excel
;;;   However, clj-excel was not avaible as jar-repo.

(def ^:dynamic *row-missing-policy* Row/CREATE_NULL_AS_BLANK)

(def ^:dynamic *data-formats* {:general 0 :number 1 :decimal 2 :comma 3 :accounting 4
                               :dollars 5 :red-neg 6 :cents 7 :dollars-red-neg 8
                               :percentage 9 :decimal-percentage 10 :scientific-notation 11
                               :short-ratio 12 :ratio 13
                               :date 14 :day-month-year 15 :day-month-name 16 :month-name-year 17
                               :hour-am-pm 18 :time-am-pm 1 :hour 20 :time 21 :datetime 22})

;; Utility Constant Look Up ()

(def Debug false)

(defn printlnD 
  "Tool for debugging. "
  [& args]
  (when Debug  (apply println args))
  nil)

(defn constantize
  "Helper to read constants from constant like keywords within a class.  Reflection powered."
  [klass kw]
  (.get (.getDeclaredField klass (-> kw name (.replace "-" "_") .toUpperCase)) Object))

(defn cell-style-constant
  ([kw prefix]
     (if (number? kw)
       (short kw)
       (short (constantize CellStyle (if prefix
                                       (str
                                        (name prefix) "-"
                                        (-> kw name
                                            (.replaceFirst (str (name prefix) "-") "")
                                            (.replaceFirst (str (name prefix) "_") "")
                                            (.replaceFirst (name prefix) "")))
                                       kw)))))
  ([kw] (cell-style-constant kw nil)))

;; Workbook and Style functions

(defn data-format
  "Get dataformat by number or create new."
  [wb sformat]
  (cond
   (keyword? sformat) (data-format wb (sformat *data-formats*))
   (number? sformat) (short sformat)
   (string? sformat) (-> wb .getCreationHelper .createDataFormat (.getFormat sformat))))

(defn set-border
  "Set borders, css order style.  Borders set CSS order."
  ([cs all] (set-border all all all all))
  ([cs caps sides] (set-border caps sides sides caps))
  ([cs top right bottom left] ;; CSS ordering
     (.setBorderTop cs (cell-style-constant top :border))
     (.setBorderRight cs (cell-style-constant right :border))
     (.setBorderBottom cs (cell-style-constant bottom :border))
     (.setBorderLeft cs (cell-style-constant left :border))))

(defn font
  "Register font with "
  [wb fontspec]
  (if (isa? (type fontspec) Font)
    fontspec
    (let [default-font (.getFontAt wb (short 0)) ;; First font is default
          boldweight (short (get fontspec :boldweight (if (:bold fontspec)
                                                        Font/BOLDWEIGHT_BOLD
                                                        Font/BOLDWEIGHT_NORMAL)))
          color (short (get fontspec :color (.getColor default-font)))
          size (short (* 20 (short (get fontspec :size (.getFontHeightInPoints default-font)))))
          name (str (get fontspec :font (.getFontName default-font)))
          italic (boolean (get fontspec :italic false))
          strikeout (boolean (get fontspec :strikeout false))
          typeoffset (short (get fontspec :typeoffset 0))
          underline (byte (get fontspec :underline (.getUnderline default-font)))]
      (or
       (.findFont wb boldweight size color name italic strikeout typeoffset underline)
       (doto (.createFont wb)
         (.setBoldweight boldweight)
         (.setColor color)
         (.setFontName name)
         (.setItalic italic)
         (.setStrikeout strikeout)
         (.setUnderline underline))))))

(defn create-cell-style
  "Create style for workbook"
  [wb & {format :format alignment :alignment border :border fontspec :font}]
  (let [cell-style (.createCellStyle wb)]
    (if fontspec (.setFont cell-style (font wb fontspec)))
    (if format (.setDataFormat cell-style (data-format wb format)))
    (if alignment (.setAlignment cell-style (cell-style-constant alignment :align)))
    (if border (if (coll? border)
                 (apply set-border cell-style border)
                 (set-border cell-style border)))
    cell-style))

;; Reading functions

(defn cell-value
  "Return proper getter based on cell-value"
  ([cell] (cell-value cell (.getCellType cell)))
  ([cell cell-type]
     (condp = cell-type
       Cell/CELL_TYPE_BLANK nil
       Cell/CELL_TYPE_STRING (.getStringCellValue cell)
       Cell/CELL_TYPE_NUMERIC (if (DateUtil/isCellDateFormatted cell)
                                (.getDateCellValue cell)
                                (.getNumericCellValue cell))
       Cell/CELL_TYPE_BOOLEAN (.getBooleanCellValue cell)
       Cell/CELL_TYPE_FORMULA {:formula (.getCellFormula cell)}
       Cell/CELL_TYPE_ERROR {:error (.getErrorCellValue cell)}
       :unsupported)))

;;(def curr-formats (atom nil))

(declare sheets)

(defn workbook
  "Create or open new excel workbook. Defaults to xlsx format."
;  ([] (new XSSFWorkbook))
  ;; CvK  added this function as the new XSSFWorkbook results in invalid excel-files
  ([] (workbook (-> (Thread/currentThread) 
                  .getContextClassLoader 
                  (.getResource "empty.xls"))))  ;; reads an empty xls (HSSF format) from the classpath (located in vinzi.tools/resources)
  ([input] (let [wb (WorkbookFactory/create (input-stream input))
;             (when @curr-formats
;               (throw (Exception. "Currently only able to open one workbook simultaneously")))
;             (let [
;                   create-style (fn [formatStr]
;                                  (let [df (-> (.createDataFormat wb)
;                                             (.getFormat formatStr))
;                                        ;; replace by create-cell-style
;                                        cellStyle (.createCellStyle wb)]
;                                    (.setDataFormat cellStyle df)
;                                    cellStyle))
;                   fmts {:dateFormat   (create-style "YYYY-MM-DD")
;                         :doubleFormat (create-style "#,##0.0")
;                         :textFormat    (create-style "@")}]
                   fmts {:dateFormat   (create-cell-style wb :format "YYYY-MM-DD")
                         :doubleFormat (create-cell-style wb :format "#,##0.0")
                         :textFormat    (create-cell-style wb :format "@")}]
;               (swap! curr-formats (fn [_] fmts)))
               {:poiWb wb
                :formats fmts
                :currSht (first (sheets wb))
                }
             )))


(defn sheets
  "Get seq of sheets."
  [wb]
  (let [wb (if (map? wb) (:poiWb wb) wb) ]
        (map #(.getSheetAt wb %1) (range 0 (.getNumberOfSheets wb)))))

(defn rows
  "Return rows from sheet as seq.  Simple seq cast via Iterable implementation."
  [sheet] 
  (let [sheet (if (map? sheet) (:currSht sheet) sheet)] ;; map workbook object to current sheet
    (seq sheet)))

(defn cells
  "Return seq of cells from row.  Simpel seq cast via Iterable implementation." 
  [row] (seq row))

(defn values
  "Return cells from sheet as seq."
  [row] (map cell-value (cells row)))

(defn lazy-sheet
  "Lazy seq of seq representing rows and cells."
  [sheet]
  (map #(map values %1) sheet))

(defn lazy-workbook
  "Lazy workbook report."
  [wb]
  (let [wb (if (map? wb) (:poiWb wb) wb)
        numSheets (.getNumberOfSheets wb)]
  (zipmap (map #(.getSheetName wb %) (range numSheets)) (map lazy-sheet (sheets wb)))))


(defn get-lazy-sheet
  "Get a lazy sheet which excludes all empty rows. 
   Empty cells are patched with nils, to ensure columns stay aligned."
  [{:keys [poiWb]} shIndex]
  (let [sheet (.getSheetAt poiWb shIndex)
        rows (seq sheet)
        unpack-row (fn [row]
                     (let [cells (seq row)
                           values (map cell-value cells)
                           ;; the iterator does not show empty ceels, so we have
                           ;; to padd nils to keep columns aligned
                           idx    (map #(.getColumnIndex %) cells)
                           nils   (map #(dec (- %1 %2)) idx (cons -1 idx))
                           cells (map #(concat (repeat %1 nil) (list %2)) nils values)]
                       (apply concat cells)))]
   (map unpack-row rows))) 

(defn get-sheet-vecs
  [wb shIndex]
  (let [ls (get-lazy-sheet wb shIndex)]
    (vec (map vec ls))))


(defn get-cell
  "Get cell within row"
  ([row col] (.getCell row col))
  ([sheet row col] (get-cell (or (.getRow sheet row) (.createRow sheet row)) col)))

;; Writing Functions

;(defn coerce
;  "Coerce cell for Java typing."
;  [v]
;  (cond
;   (number? v) (double v)
;   (or (symbol? v) (keyword? v)) (name v)
;   :else v))



;; only an internal function used by merge-rows?
;(defn set-cell-aux
;  "Set cell at specified location with value."
;  ([cell value] 
;    (let [cell-spec (fn 
;                      ;;"Get a cell definition consisting of a value and a format to be applied. Format might be nill"
;                      [value]
;                      (println "value " value " has type " (type value))
;                      (let [tp (type value)]
;                        (cond 
;                          (= tp java.lang.String) [value Cell/CELL_TYPE_STRING (:textFormat @curr-formats)]
;                          (#{clojure.lang.Keyword clojure.lang.Symbol} tp)  [(name value) Cell/CELL_TYPE_STRING (:textFormat @curr-formats)]
;                          (#{java.lang.Integer java.lang.Long} tp ) [(double value) Cell/CELL_TYPE_NUMERIC nil]
;                          (= tp java.lang.Boolean) [value Cell/CELL_TYPE_BOOLEAN nil]
;                          (#{java.lang.Double java.lang.Float} tp) [value Cell/CELL_TYPE_NUMERIC (:doubleFormat @curr-formats)]
;                          ;; dates are stored as a numeric with some additional formatting
;                          (#{java.util.Date java.sql.Date} tp)  [value Cell/CELL_TYPE_NUMERIC (:dateFormat @curr-formats)]
;                          ;;    java.sql.Date  [(java.util.Date. (.getTime value)) (:dateFormat @curr-formats)]  ;; converting to java-util.date
;                          ;;(= tp java.sql.Date)  [value  Cell/CELL_TYPE_NUMERIC (:dateFormat @curr-formats)]
;                          )))
;          ]
;    (try
;      (when value
;        (let [[value tpe fmt] (cell-spec value)]
;          (println "set cell to value: " value " and format " fmt)
;          (.setCellValue cell value)
;          (when tpe 
;            (.setCellType cell tpe))
;          (when fmt
;            (.setCellStyle cell fmt))))
;      (catch Throwable t
;        (error "(clj-excel/set-cell-aux): type error on value " value " of " (type value))
;        (let [[coerced fmt] (cell-spec value)]
;          (error "(clj-excel/set-cell-aux): which was coerced to value " coerced " of " (type coerced) " and format="fmt)) 
;        (throw t))))) ;; rethrow it after having done the additional reporting
;  ([row col value] (set-cell-aux (or (get-cell row col) (.createCell row col)) value))
;  ([sheet row col value]
;    (println "enter (set-cell-aux " sheet " " row " " col " " value ") while (.getRow sheet row) returns: " (.getRow sheet row))
;    (set-cell-aux (or (.getRow sheet row) (.createRow sheet row)) col value)))

;; only an internal function used by merge-rows?
(defn set-cell
  "Set cell in the current sheet of workbook at specified location with value."
  [workbook row col value]
  {:pre [(map? workbook)]}
    (let [{:keys [currSht formats]} workbook  ;; get current sheet and formats of this workbook
          set-cell-aux (fn [cell value] 
                         (let [cell-spec (fn 
                                           ;;"Get a cell definition consisting of a value and a format to be applied. Format might be nill"
                                           [value]
                                           (printlnD "value " value " has type " (type value))
                                           (let [tp (type value)]
                                             (cond 
                                               (= tp java.lang.String) [value Cell/CELL_TYPE_STRING (:textFormat formats)]
                                               (#{clojure.lang.Keyword clojure.lang.Symbol} tp)  [(name value) Cell/CELL_TYPE_STRING (:textFormat formats)]
                                               (#{java.lang.Integer java.lang.Long} tp ) [(double value) Cell/CELL_TYPE_NUMERIC nil]
                                               (= tp java.lang.Boolean) [value Cell/CELL_TYPE_BOOLEAN nil]
                                               (#{java.lang.Double java.lang.Float} tp) [value Cell/CELL_TYPE_NUMERIC (:doubleFormat formats)]
                                               ;; dates are stored as a numeric with some additional formatting
                                               (#{java.util.Date java.sql.Date} tp)  [value Cell/CELL_TYPE_NUMERIC (:dateFormat formats)]
                                               ;;    java.sql.Date  [(java.util.Date. (.getTime value)) (:dateFormat @curr-formats)]  ;; converting to java-util.date
                                               ;;(= tp java.sql.Date)  [value  Cell/CELL_TYPE_NUMERIC (:dateFormat @curr-formats)]
                                               )))
                               ]
                           (try
                             (when value
                               (let [[value tpe fmt] (cell-spec value)]
                                 (printlnD "set cell to value: " value " and format " fmt)
                                 (.setCellValue cell value)
                                 (when tpe 
                                   (.setCellType cell tpe))
                                 (when fmt
                                   (.setCellStyle cell fmt))))
                             (catch Throwable t
                               (error "(clj-excel/set-cell-aux): type error on value " value " of " (type value))
                               (let [[coerced fmt] (cell-spec value)]
                                 (error "(clj-excel/set-cell-aux): which was coerced to value " coerced " of " (type coerced) " and format="fmt)) 
                               (throw t))))) ;; rethrow it after having done the additional reporting
          set-create-cell (fn [row col value] 
                            (set-cell-aux (or (get-cell row col) (.createCell row col)) value))
          set-create-cell-row (fn [sheet row col value]
                                (printlnD "enter (set-cell/set-create-cell-row " sheet " " row " " col " " value ") while (.getRow sheet row) returns: " (.getRow sheet row))
                                (set-create-cell (or (.getRow sheet row) (.createRow sheet row)) col value))
          ]
    (set-create-cell-row currSht row col value)))


(defn merge-rows
  "Add rows at end of sheet (or within sheet starting at row start) ."
  [wb start rows]
  {:pre [(map? wb)]}
  (doall
    (map
      (fn [rownum vals] (doall (map #(set-cell wb rownum %1 %2) (iterate inc 0) vals)))
      (range start (+ start (count rows)))
      rows)))

(defn build-sheet
  "Build sheet from seq of seq (representing cells in row of rows)."
  [wb sheetname rows]
  (let [sheet (if sheetname
                (.createSheet wb sheetname)
                (.createSheet wb))]
    (merge-rows sheet 0 rows)))

(defn build-workbook
  "Build workbook from map of sheet names to multi-dimensional seqs (ie a seq of seq)."
  ([wb wb-map]
     (doseq [[sheetname rows] wb-map]
       (build-sheet wb (str sheetname) rows))
     wb)
  ([wb-map] (build-workbook (workbook) wb-map)))

(defn save
  "Write worksheet to output-stream as coerced by OutputStream."
  [wb path]
  {:pre [(map? wb)]}
  ;; CvK: added a with-open instead of a let
  (with-open [out (output-stream path)]
    (.write (:poiWb wb) out)))
;;    out))


;; CvK:  Added functions for an intermediate byte-stream
(defn save-to-byteStream
  "Write a workbook to a byte-stream. The bytestream can be written to an output-stream later (used for cdpPlugin interface"
  [wb]
  {:pre [(map? wb)]}
  (let [bs (java.io.ByteArrayOutputStream.)]
    ;;
    (.write (:poiWb wb) bs)
;;    (.write bs (.getBytes wb))
  bs))

(defn write-bytestream [bs path]
  (with-open [out (output-stream path)]
    (.write out (.toByteArray bs))))


(comment


(def tstData2 {
               :double 1.23
;;               :float  (float 4.5) ;; float is not accepted.
               :string "test-string"
               :pseudo "<span class=\"pseudof\" key=\"ABC\"/>"
               :java_date  (java.util.Date.)
               :sql_date   (java.sql.Date. (.getTime (java.util.Date.)))
    ;;           :int        (int 1)
               :long       (long -10)
               })

(def tstRows1 (repeat 10 ["a" 1 2.0]))
(def tstRows2 (list (keys tstData2) (vals tstData2)))
  

  ;; OLD example
;  (defn test-it []
;    (let [nb (workbook)] 
;      (-> (first (sheets nb))
;        (merge-rows  1 tstRows2))
;      (save nb "/tmp/newfile.xls")))
  ;;new example
  (defn test-it []
    (let [nb (workbook)] 
        (merge-rows nb 1 tstRows2)
      (save nb "/tmp/newfile.xls")))
  
) ;; end comment


         
         
