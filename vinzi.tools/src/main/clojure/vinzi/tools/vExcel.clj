(ns vinzi.tools.vExcel
  (:use  [clojure.tools.logging :only [debug warn error]])
  (:require [clojure.java.io :as io])
  (:import ;[org.apache.poi.xssf.usermodel XSSFWorkbook]
           [org.apache.poi.hssf.usermodel HSSFWorkbook]
           [org.apache.poi.ss.usermodel Row Cell DateUtil WorkbookFactory CellStyle Font]))


;;;;
;;;   Based on (copy of)    https://github.com/mebaran/clj-excel
;;;   However, 
;;;      -  clj-excel was not avaible as jar-repo.
;;;      -  preferred a different interface 
;;;      -  default to HSSF instead of XSSF 
;;;      -  better documentation (and checks)
;;;      -  Smaller library, only containing the parts I need 

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  The basic workbook of this library is a hashmap with keys (:poiWb :sheets :formats)
(def ^:dynamic *row-missing-policy* Row/CREATE_NULL_AS_BLANK)

(def ^:dynamic *data-formats* {:general 0 :number 1 :decimal 2 :comma 3 :accounting 4
                               :dollars 5 :red-neg 6 :cents 7 :dollars-red-neg 8
                               :percentage 9 :decimal-percentage 10 :scientific-notation 11
                               :short-ratio 12 :ratio 13
                               :date 14 :day-month-year 15 :day-month-name 16 :month-name-year 17
                               :hour-am-pm 18 :time-am-pm 1 :hour 20 :time 21 :datetime 22})


(def Debug false)

(defn printlnD 
  "Tool for debugging. "
  [& args]
  (when Debug  (apply println args))
  nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; generating a workbook inclusding some styles and formats

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


(defn get-sheet 
  "Get the sheet based on it's name (string) or it's index-number."
  [{:keys [poiWb] :as wb} id]
  (if (number? id)
    (.getSheetAt poiWb (int id))
    (.getSheet poiWb (str id))))
    

(defn get-sheet-names
  "Get a vector with all sheets-names from workbook (can also operate on base poi-workbook)."
  [wb]
  (let [wb (if (map? wb) (:poiWb wb) wb) ]
     (vec (map #(.getSheetName wb %1) (range 0 (.getNumberOfSheets wb))))))


(defn workbook
  "Create or open new excel workbook. Defaults to xls format (old style excel).
   (Using HSSF (.xls) instead of  XSSF (.xlsx) as the poi library seems more stable for HSSF."
  ([] (workbook (-> (Thread/currentThread) 
                  .getContextClassLoader 
                  ;; reads an empty xls (HSSF format) from the classpath (located in vinzi.tools/resources)
                  ;; (generation os empty files sometimes gave issues.)
                  (.getResource "empty.xls"))))  
  ([input] (let [wb (WorkbookFactory/create (io/input-stream input))
                   fmts {:dateFormat   (create-cell-style wb :format "YYYY-MM-DD")
                         :doubleFormat (create-cell-style wb :format "#,##0.0")
                         :textFormat    (create-cell-style wb :format "@")}]
               {:poiWb wb
                :formats fmts
                }
             )))


;;;; reading the excel
(defn cell-value
  "Return proper getter based on cell-value.
   Formula's are packaged as a hash-map."
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

(defn get-lazy-sheet
  "Get a lazy sheet which excludes all empty rows. 
   Empty cells are patched with nils, to ensure columns stay aligned."
  [{:keys [poiWb sheets] :as wb} sheetId]
  (let [sheet (get-sheet wb sheetId) 
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
  "Return sheets as a vector of vectors (forces realisation.)"
  [wb shIndex]
  (let [ls (get-lazy-sheet wb shIndex)]
    (vec (map vec ls))))


(defn get-cell
  "Get cell within row. Create the Row and/or the Cell if they do not exist yet.
   Currently only used by set-cell function."
  ([row col] (.getCell row col))
  ([sheet row col] (get-cell (or (.getRow sheet row) (.createRow sheet row)) col)))



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
  "Add rows at end of sheet identified by 'sheetId' (default sheet 0)
  (This operation overwrites existing content)."
  ([wb start rows]  ;; default to insert in first sheet
   (merge-rows wb 0 start rows))
  ([wb sheetId start rows]
    (let [wb (assoc wb :currSht (get-sheet sheetId))]
      (doall
        (map
          (fn [rownum vals] (doall (map #(set-cell wb rownum %1 %2) (iterate inc 0) vals)))
          (range start (+ start (count rows)))
          rows)))))


(defn build-sheet
  "Build sheet from seq of seq (representing cells in row of rows without gaps).
   Return nothing."
  [{:keys [poiWb] :as wb} sheetName rows]
  (let [sheet (if (seq sheetName)
                (.createSheet poiWb sheetName)
                (.createSheet poiWb))
        sheetName (if (seq sheetName) 
                    sheetName 
                    ;; assume new Sheet is in last postion
                    (dec (.getNumberOfSheets poiWb)))]
    (merge-rows wb sheetName 0 rows)))


(defn save
  "Write worksheet to output-stream as coerced by OutputStream."
  [{:keys [poiWb] :as wb} path]
  (with-open [out (io/output-stream path)]
    (.write poiWb out)))


(defn save-to-byteStream
  "Write a workbook to a byte-stream. The bytestream can be written 
  to an output-stream later (used for cdpPlugin interface"
  [{:keys [poiWb] :as wb}]
  (let [bs (java.io.ByteArrayOutputStream.)]
    (.write poiWb bs)
  bs))

(defn write-bytestream 
  "Write a bytestream (generated by save-to-byteStream) to a file."
  [bs path]
  (with-open [out (io/output-stream path)]
    (.write out (.toByteArray bs))))







