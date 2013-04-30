(ns clj-excel.core
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

(defn workbook
  "Create or open new excel workbook. Defaults to xlsx format."
;  ([] (new XSSFWorkbook))
  ;; CvK  added this function as the new XSSFWorkbook results in invalid excel-files
  ([] (workbook (-> (Thread/currentThread) 
                  .getContextClassLoader 
                  (.getResource "empty.xls"))))  ;; reads an empty xls (HSSF format) from the classpath (located in vinzi.tools/resources
  ([input] (WorkbookFactory/create (input-stream input))))

(defn sheets
  "Get seq of sheets."
  [wb] (map #(.getSheetAt wb %1) (range 0 (.getNumberOfSheets wb))))

(defn rows
  "Return rows from sheet as seq.  Simple seq cast via Iterable implementation."
  [sheet] (seq sheet))

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
  (zipmap (map #(.getSheetName %1) wb) (map lazy-sheet (sheets wb))))

(defn get-cell
  "Sell cell within row"
  ([row col] (.getCell row col))
  ([sheet row col] (get-cell (or (.getRow sheet row) (.createRow sheet row)) col)))

;; Writing Functions

(defn coerce
  "Coerce cell for Java typing."
  [v]
  (cond
   (number? v) (double v)
   (or (symbol? v) (keyword? v)) (name v)
   :else v))


;; only an internal function used by merge-rows?
(defn set-cell
  "Set cell at specified location with value."
  ([cell value] 
    (try
      (.setCellValue cell (coerce value))
      (catch Throwable t
        (error "(clj-excel/set-cell): type error on value " value " of " (type value))
        (let [coerced (coerce value)]
          (error "(clj-excel/set-cell): which was coerced to value " coerced " of " (type coerced))) 
        (throw t)))) ;; rethrow it after having done the additional reporting
  ([row col value] (set-cell (or (get-cell row col) (.createCell row col)) value))
  ([sheet row col value]
    (println "enter (set-cell " sheet " " row " " col " " value ") while (.getRow sheet row) returns: " (.getRow sheet row))
    (set-cell (or (.getRow sheet row) (.createRow sheet row)) col value)))

(defn merge-rows
  "Add rows at end of sheet."
  [sheet start rows]
  (doall
   (map
    (fn [rownum vals] (doall (map #(set-cell sheet rownum %1 %2) (iterate inc 0) vals)))
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
  "Build workbook from map of sheet names to multi dimensional seqs (ie a seq of seq)."
  ([wb wb-map]
     (doseq [[sheetname rows] wb-map]
       (build-sheet wb (str sheetname) rows))
     wb)
  ([wb-map] (build-workbook (workbook) wb-map)))

(defn save
  "Write worksheet to output-stream as coerced by OutputStream."
  [wb path]
  ;; CvK: added a with-open instead of a let
  (with-open [out (output-stream path)]
    (.write wb out)))
;;    out))


;; CvK:  Added functions for an intermediate byte-stream
(defn save-to-byteStream
  "Write a workbook to a byte-stream. The bytestream can be written to an output-stream later (used for cdpPlugin interface"
  [wb]
  (let [bs (java.io.ByteArrayOutputStream.)]
    ;;
    (.write wb bs)
;;    (.write bs (.getBytes wb))
  bs))

(defn write-bytestream [bs path]
  (with-open [out (output-stream path)]
    (.write out (.toByteArray bs))))


(comment
  
  ;; example
  (def nb (ce/workbook))
  (-> (first (ce/sheets nb))
    (ce/merge-rows  5 (repeat 10 ["a" 1 2.0])))
  (ce/save nb "/tmp/newfile.xls")
)


         
         