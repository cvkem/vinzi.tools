(ns vinzi.tools.vSql
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as sql]
            [vinzi.tools
             [vDateTime :as vDate]
             [vExcept :as vExcept]]))



(def defaultDb {
         :classname "org.postgresql.Driver"
         :subprotocol "postgresql"
         :subname  "//localhost/Tdat"
         :user  "tos"
         :password "tos-user"})


;; specify the cursor to be used to run over the data.
;; data is returned as a lazy sequence, so prepare to do a (doall ) to materialize data before closing the connection
(def DefSqlOpts {:fetchsize 1000
                 :result-type :forward-only})

(defn printSQLExcept
  "Print report for an SQL exception
   (including one step higher in the exception-chain)."
  [location e]
  (vExcept/report location e))  
;  (error (with-out-str
;           (println "Exception of type: " (class e))
;           (print-stack-trace (root-cause e))
;           (println "Exception caught at location " location)
;           (println "Message: " (.getMessage e))
;           (when (isa? (class e) java.sql.SQLException)
;             (println "SQL-related Exception details:")
;             (println "ErrorCode: " (.getErrorCode e))
;             (println "SQLState:  " (.getSQLState e))
;             (when-let [n (.getNextException e)]
;               (println "Next-message: " (.getMessage n))
;               (println "Next-errorcode: " (.getErrorCode n)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   functions to produce (double-)quoted strings for Postgres.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn qs
  "Surround a string by double quotes (postgres requires this), 
   unless it is already double-quoted (no trimming of spaces on argument)."
  [s]
  (let [s (str s)]
    (if (seq s)
      (let [startQ (= (first s) \")
            endQ (= (last s) \")]
        (if (or startQ endQ)
          (if (and startQ endQ)
            s    ;; string is double quoted already
            (throw (Exception. (str "(vSql/qs) unbalanced quotes in argument: " s ))))
          (str "\"" s "\"")))
      "\"\"")))

(defn qsp
  "Create a double-qouted string pair for schema.table or table.field."
  [schema nme]
  (if (seq schema)
    (str (qs schema) "." (qs nme))
    (qs nme)))   ;; if now schema passed "" or nil, then only return table name (defaults to 'public')



(defn sqs 
  "Change s to a single-quoted sting, unless it is already single-quoted (no trimming of spaces on argument).
    Fix a string for usage in sql query strings. All single quotes (') will be expanded to ''."
  [s]
  (let [s (str s)]
    (if (seq s)
      (let [startQ (= (first s) \')
            endQ   (= (last s) \')]
        (if (or startQ endQ)
          (if (and startQ endQ)
            s    ;; string is single quoted already
            (throw (Exception. (str "(vSql/sqs) unbalanced quotes in argument: " s ))))
          (-> s
            (str/replace #"'" "''")  ;; replace single quotes
            (#(str "'" % "'") ))))
      "''")))

(defn strip-dQuotes 
  "Strip the double-quotes from a string. Throw an exception when quotes are unbalanced. (does no trim on string)"
  [s]
    (let [s (str s)]
    (if (seq s)
      (let [startQ (= (first s) \")
            endQ (= (last s) \")]
        (if (or startQ endQ)
          (if (and startQ endQ)
            (apply str (drop-last (rest s)))    ;; string is double quoted, so strip first and last
            (throw (Exception. (str "(vSql/qs) unbalanced quotes in argument: " s ))))
          s))  ;; no double quotes, so return s
      "\"\"")))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Perform clojure-actions on a database while retaining the
;; caps (lower- and upper-case) of columnkeys (in the hash-maps).
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def pgNaming {:keyword identity :entity qs})

(defn with-db-caps "Run a function 'action' with arguments 'args' within connection 'db', and maintains caps of columnlabels/keys."
  [db action & args]
      (sql/with-connection db
        (sql/with-naming-strategy pgNaming
;;        (trace "*as-str* = " #'clojure.java.jdbc/*as-str*)
;;        (trace "*as-key* = " #'clojure.java.jdbc/*as-key*)
          (apply action args))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Generate database connection records and db-key
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn db-key "Translate a fieldname to a string that retrieves the field from the jdbc hashmaps." 
  [x]
  {:pre [(string? x)]}   ;; do not pass in keywords otherwise the :: will be duplicated as lower-case returns ":xyz" for keyword :XYz
  (keyword (str/lower-case x)))


(defn generate-db
  "Generate a db-connection record based on the provided parameters supplemented with parameters from 'defaultDb'. 
     (A optional url-parameter is assumed to have a shape like: [subprotocol]//[db-host]:[db-port]/[database], 
           db-port is optional and a 'jdbc:' prefix is removed."
  [{:keys [classname subprotocol
                          db-host db-port db-name
                          user password
                          url] :as dbPars}]
  (let [lpf "(vSql/generate-db): " ]
    (when (not (and (or db-name url) user password))
      (vExcept/throw-except lpf "Definition of 'db-name', 'user', and 'password' is manditory in generate-db (where 'db-name' can be replaced by 'url')."))
    (let [baseRec (if url
                    (let [url (str/trim url)
                          jdbcPrefix "jdbc:"
                          url (if (.startsWith url jdbcPrefix) (apply str (drop (count jdbcPrefix) url)) url)]
                      ;; parse url as: [subprotocol]//[db-host]:[db-port]/[database]
                      (if-let [parsedUrl (re-find #"([\w:-_]*)//([\w\._-]*):(\d*)/(\w*)" url)]
                        (zipmap [:url :subprotocol :db-host :db-port :db-name] parsedUrl)
                        ;; parse url as: [subprotocol]//[db-host]/[database]    (no db-port)
                        (if-let [parsedUrl (re-find #"([\w:-_]*)//([\w\._-]*)/(\w*)" url)]
                          (zipmap [:url :subprotocol :db-host :db-name] parsedUrl))))
                    {})
          ;; strip trailing \: in subprotocol   (needed as hsql subprotocol is hsqldb:hsql
          baseRec  (if-let [subprotocol (:subprotocol baseRec)]
                     (if (= (last subprotocol) \:)  
                       (assoc baseRec :subprotocol (apply str (take (dec (count subprotocol)) subprotocol)))  
                       baseRec)
                     baseRec)
          defaults  {:classname    (:classname defaultDb)
                     :subprotocol  (:subprotocol defaultDb)
                     :db-host       "localhost"}
          ;; patch the defaults for values that do not exist
          {:keys [classname subprotocol
                          db-host db-port db-name
                          user password]}  (into (into defaults baseRec) dbPars)
          ]
      {:classname    classname
       :subprotocol  subprotocol
       :subname      (str "//" db-host (if db-port (str ":" db-port) "") "/" db-name)
       :user         user
       :password     password})))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; finding information on the connection
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn get-current-database 
  "Get the name of the database of the current connection."
  []
  (.getCatalog (sql/connection)))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  auxiliary functions for testing purpose
;;  (shortened version of vinzi.tools.sqlTools/table-exists, to prevent dependency)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn schema-exists 
  "Check wether a schema exists (using current db-connection)"  
  [schema]
  (let [qry (str "SELECT count(*) AS cnt "
		 " FROM information_schema.schemata "
		 " WHERE schema_name = '" schema "';")]
    (sql/with-query-results res [qry]
      (assert (= (count res) 1))
      (> (:cnt (first res)) 0))))


(defn table-exists 
  "Check wether a 'table' exists in 'schema' (using current db-connection)"
  ;; the alternative would be to inspect pg_tables (postgres specific)
  [schema table]
  (let [schema (strip-dQuotes schema)
        schema (if (seq schema) schema "public")  ;; added to have 'public' as default schema
        table  (strip-dQuotes table)
        qry (format "select * from information_schema.tables WHERE table_name LIKE '%s' AND table_schema = '%s';" table schema)]
     (sql/with-query-results res [qry]
            (let [outcome (if (seq res)
                            true
                            false)]
              outcome))))


(defn table-length 
  "Calculate the number of rows in a table (or view)"
  [schema table]
  (let [lpf "(table-length): "]
    (try
      (let [qry (str "SELECT count(*) AS cnt FROM " (qsp schema table) ";")]
        (sql/with-query-results res [qry]
                                (assert (= (count res) 1))
                                (:cnt (first res))))
      (catch Exception e
        (vExcept/report lpf e)
        0))))

(defn trunc-table 
  "Truncate a table and returns nil on succes and an exception-message on failure (exception has been reported first)"
  [schema table]
  (let [lpf "(trunc-table): "]
    (try
      (let [qry (str "DELETE FROM " (qsp schema table) ";")]
        (sql/do-commands qry))
      (catch Exception e
        (vExcept/report lpf e)
        (.getMessage e)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  auxiliary functions for creating database-entities
;;  (These functions all use the two-parameter interface 
;;    with schema separated from seqNme)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn sequence-exists "Check if sequence exists." [schema seqNme]
  (let [lpf "(sequence-exists): "
        seqNme (strip-dQuotes seqNme)
        schema (strip-dQuotes schema)
        qName (qsp schema seqNme)]
    (let [qry (str "select * from information_schema.sequences "
                   "\nWHERE sequence_name = " (sqs seqNme)
                   " AND sequence_schema = " (sqs schema) ";")]
      (trace lpf "  check whether " qName " is an existing sequence"
             "\n\t using query: " qry)
      (sql/with-query-results res [qry]
                              (let [outcome (if (seq res)
                                              true
                                              false)]
                                ;;(trace "query returns: " res)
                                (debug lpf "sequence " qName " exists = " outcome)
                                outcome)))))


(defn create-sequence "Create a sequence only if it does not exist." 
  [schema seqNme]
  (let [lpf "(create-sequence): "
        qName (qsp schema seqNme)]
    (if (not (sequence-exists schema seqNme))
      (let [qry (str "CREATE SEQUENCE " qName)]
        (trace lpf " running query: " qry)
        (sql/do-commands qry))
      (debug lpf " sequence " qName " exists already (not created)"))))


(defn add-primary-key 
  "Add a primary key to to a tables. Assuming primary-key is a string or a  sequence of strings that corresponds to valid field-names."
  [schema tblNme primaryKey]
  (let [lpf "(primary-key): "
        tblNme (strip-dQuotes tblNme)  ;; needed for primary key
        qTblNme (qsp schema tblNme)
        _ (trace lpf " adding primary key " primaryKey)
        primaryKey (if (string? primaryKey)
                      (qs primaryKey)
                      (str/join "," (map qs primaryKey)))
        _ (trace lpf "primary key changed to: " primaryKey)
        _ (assert (string? primaryKey))
;;        table (:nme (split-qualified-name qTblNme))
        qry (str "ALTER TABLE "
                 qTblNme
                 " ADD CONSTRAINT "
                 (qs (str tblNme "_pk "))
                 " PRIMARY KEY (" primaryKey ");")]
    (debug lpf "adding Primary key via query: " qry)
    (sql/do-commands qry)))



;; DEPRECATED:  use get-sql-field-defs
;(defn get-create-field-str-DEPR "Return fieldsnames selected by 'sel' as quotes strings 
;  followed by the rest of the string (type, default-values, etc..) 
;  and interposed by commas(for usage in a create statement).
;   Deprecated: use get-sql-field-defs."
;  [flds sel]
;  (let [lpf "(get-create-field-str): "
;        fldNames (map #(nth flds %) sel)
;        _ (trace lpf "selected fldNames: " fldNames)
;        fldNames (letfn [(quote-first [org]
;                                      (let [org (str/trim org)]
;                                        (if (= (first org) \")
;                                          org  ;; return unmodified
;                                          (let [s (str/split org #"\s+")
;                                                head (qs (first s))
;                                                tail (str/join " " (rest s))]
;                                            (str head "\t" tail)))))]
;                        (map quote-first fldNames))
;        _ (trace "after quoting fldNames: " fldNames)
;        fldNames  (interpose ", " fldNames)]
;    (apply str fldNames)))

(defn get-select-field-str 
  "Return fieldsnames selected by 'sel' as quotes strings followed interposed by commas(for usage in a select statement)."
  ;; TODO: We should add an optional prefix to disambiguate when concatenated with other fields (Needed by vinzi.eis.cvrm.core)
  ([flds sel]
    (get-select-field-str flds sel nil))
  ([flds sel  prefix]
  (let [lpf "(get-select-field-str): "
        fldNames (map #(nth flds %) sel)
        _ (trace lpf "selected fldNames: " fldNames)
        fldNames (letfn [(quote-first [org]
                            (let [org (str/trim org)]
                              (if (= (first org) \")
                                ;; return the field name
                                (re-find #"\"[\w\s]+\"" org)    ;; different from get-create-field-str 
                                (let [s (str/split org #"\s+")
                                      head (qs (first s))]
                                  head))))]                     ;; different from get-create-field-str
                   (map quote-first fldNames))
           _ (trace "after quoting fldNames: " fldNames)
           ;; add prefix when specified.
           fldNames (if (string? prefix)
                     (map #(str prefix "." %) fldNames)
                     fldNames)
;;           fldNames  (interpose ", " fldNames)
           ]
       (str/join ", " fldNames))))


(defn get-sql-field-defs 
  "Get the field definitions as used in a create-statement. The fields should be either a sequence of strings, or 
    a series of records with shape keys :nme, :tpe :constraint :default. 
    Field-definitions without a type a considered to be a (multi-column) constraint.
    Examples of field-definitions:
       {:nme \"id\"  :tpe :serial :constraint \"PRIMARY KEY\"}
       {:nme \"used\"  :tpe :bool :default \"FALSE\"}
       {:nme \"model\" :tpe :string  :default \"\"}
       {:nme :omzet :tpe :double  :default 0.0}
     and example of a multi-column constraint (no :tpe attribute)
       {:nme \"tbl_pk\"  :constraint \"PRIMARY KEY (jaar , kosten_plaats )\"} "
  [flds]
  {:pre [(sequential? flds)]}
  (let [lpf "(get-sql-field-defs): "
        ;; processing sting elements
        quote-first (fn [org]
                      ;; quote the first word of a string (assumed to be the field-name)
                      (let [org (str/trim org)]
                        (if (= (first org) \")
                          org  ;; return unmodified
                          (let [s (str/split org #"\s+")
                                head (qs (first s))
                                tail (str/join " " (rest s))]
                            (str head "\t" tail)))))
        ;; processing map elements
        boolean? (fn [x] (= (type x) java.lang.Boolean))
        get-default  (fn [default]
                       (cond 
                         (boolean? default) (if default "TRUE" "FALSE")
                         (string? default)  (if (= (str/trim (str/lower-case default)) "now()")
                                              "now()"   ;; asume it is a default for a date or timestamp
                                              (sqs default))
                         (number? default)   (str default)
                         (nil? default)     "NULL"   ;; nill translated to th unquoted string NULL
                         ;;  add a date type and handling of now() via special constants of hashmaps
                         :else (vExcept/throw-except lpf "default value: " default " could not be interpreted as boolean, string or number")))
        get-type (fn [tpe]
                   (case tpe
                     (:string :text) "TEXT"
                     (:integer :int) "INTEGER"
                     :serial         "serial"   ;; serial should not be in capitals
                     (:bool :boolean) "BOOLEAN"
                     (:double :dbl)  "DOUBLE PRECISION"
                     (:date)         "DATE"
                     (:timestamp)    "TIMESTAMP"  ;; is TIMESTAMP without TIMEZONE in Postgres as SQL standard requires 
                     tpe
                     ))
        process-descr    (fn [fld]
                           (let [{:keys [nme tpe constraint default]} fld
                                 allowedKeys #{:nme :tpe :constraint :default}
                                 unknownKeys (set/difference (set (keys fld)) allowedKeys)]
                             (when (seq unknownKeys)
                               (vExcept/throw-except lpf "Unknown keys: " unknownKeys "in field " fld " (only allowed keys are: " allowedKeys ")"))
                             (when-not nme 
                               (vExcept/throw-except lpf "field :nme is manditory. Received: " fld))
                             (when-not (or tpe constraint) 
                               (vExcept/throw-except lpf "either :tpe or :constraint should be set: " fld))
                             (if tpe  ;; if tpe set it is a field, otherwise a (multi-column) constraint
                               (str (qs (name nme)) "\t" (get-type tpe) 
                                    (when constraint (str " " constraint))
                                    (when default (str " DEFAULT " (get-default default))))
                               (str "CONSTRAINT " (qs (name nme)) constraint))))  ;; TODO: improve the type-sensitivity of constraints.
        create-field-def (fn [fld]
                           (let [tpe (type fld)]
                             (cond 
                               (string? fld) (quote-first fld)
                               (map? fld)    (process-descr fld)
                               :else (vExcept/throw-except lpf "could not process field " fld " of type " (type fld)))))]
    (when (some string? flds)
      (warn lpf "string-definitions detected [DEPRECATED]"))
    (str/join ", " (map create-field-def flds))))


;; added on June 8  (based on vinzi.olap4Clj.eisBedr.eisProd/get-col-defs
(defn derive-col-defs
  "Get the database definition of records similar to 'rec'. Take care it does not contain nil-values."
  [rec]
  (let [derive-def (fn [[k v]]
			{:nme k
			 :tpe (cond
				(= (type v) java.lang.Integer) :int
				(= (type v) java.lang.Long) :int     ;; TODO: check whether this always fits
				(number? v) :double
				(string? v) :string
				(#{java.util.Date java.sql.Date} (type v)) :Date
				
				)})]
    ;; TODO: make a variant of this function (or change this function to accept a sequence of records
    ;;  allows skipping first records if these contain nils (as we can not derive type-information from nil-values)
    (map derive-def rec)))

(defn check-create-schema
  "Create schema if it does not exist yet in current database."
  [schema]
  (let [lpf "(check-create-schema): "]
    (when-not (schema-exists schema)
      (debug lpf "Creating schema " (qs schema) " on connection "
             (get-current-database))
      (sql/do-commands (str "CREATE SCHEMA " (qs schema) ";")))))


(defn create-table "Create a table 'schema'.'tblName' with fields 'fields' if it does not exist yet. 
  If 'dropIt' is true than it tries to drop the table first via a drop CASCADE!
  The 'fields'-parameter is a list of field definition strings (including attributes)."
  [schema tblNme fields dropIt]
  (let [lpf "(create-table): "
        qTblNme (qsp schema tblNme)
        ;;flds (get-create-field-str fields (range (count fields)))
        flds (get-sql-field-defs fields)
        drop (format "DROP TABLE IF EXISTS %s CASCADE;" qTblNme)
        qry (format "CREATE TABLE %s (%s);"
                    qTblNme flds) ]
    (when dropIt
      (debug lpf "trying to drop table: " qTblNme)
      (sql/do-commands drop))

    (if (table-exists schema tblNme)
      (debug lpf "Table: " qTblNme " exists already (abort create proces)")
      (do
        (debug lpf "creating table" qTblNme)
        (trace lpf "create query: " qry)
        (sql/do-commands qry)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;   Functions to drop tables and views and to clear a complete schema.
;;

(defn do-commit "Do a commit on the current connection." 
  []
  (sql/do-commands "COMMIT;"))

(defn drop-it  
  "Drop an item and commit it unless 'commitIt' = false."
  ([tType schema tName]
     (drop-it tType schema tName true))
  ([tType schema tName commitIt]
     (let [qry  (format "DROP %s IF EXISTS \"%s\".\"%s\" CASCADE;"
                        tType schema tName)]
       (debug " execute drop-qry: " qry)
       (sql/do-commands qry)
       (when commitIt
         (do-commit)))))

(defn drop-tables-and-views 
  "Remove all tables and view from schema via a CASCADED DROP. 
   Schema might also be a mask (using %)." 
  [schema]
  (let [schema (strip-dQuotes schema)
        qry (str "SELECT table_type, table_schema, table_name "
                 " FROM information_schema.tables "
                 " WHERE table_schema LIKE '" schema "';")]
    (sql/with-query-results res [qry]
      (doseq [item res]
        (let [tType (if (= (:table_type item) "VIEW")
                      "VIEW" "TABLE")
              schema (:table_schema item)
              tblNme (:table_name item)]
          (drop-it tType schema tblNme)
          (when (table-exists schema tblNme)
            (error "failed to drop " schema "." tblNme))
          )))))

(defn drop-sequences 
  "Remove all sequences of schema via a CASCADED DROP. 
   Schema might also be a mask (using %)." 
  [schema]
  (let [schema (strip-dQuotes schema)
        qry (str "SELECT sequence_schema, sequence_name "
                 " FROM information_schema.sequences "
                 " WHERE sequence_schema LIKE '" schema "';")]
    (sql/with-query-results res [qry]
      (doseq [item res]
          (drop-it "SEQUENCE" (:sequence_schema item) (:sequence_name item))))))

(defn drop-routines 
  "Remove all routines of schema via a CASCADED DROP. 
   Schema might also be a mask (using %)." 
  [schema]
  (let [schema (strip-dQuotes schema)
        qry (str "SELECT routine_schema, routine_name "
                 " FROM information_schema.routines "
                 " WHERE routine_schema LIKE '" schema "';")]
    (trace "Routines can not be dropped, unless you know the parameters")
    (sql/with-query-results res [qry]
      (doseq [item res]
          (drop-it "FUNCTION" (:routine_schema item) (:routine_name item))))))

(defn clear-schema [schema]
  (debug "Dropping tables-and-views")
  (drop-tables-and-views schema)
  (debug "Dropping sequences")
  (drop-sequences schema)
  (warn "Routines (functions) can not be dropped "
        "unless you know the parameters.\n"
        "so, rely upon the CREATE OR REPLACE FUNCTION for this.")
;;  (drop-routines schema)
  )


;;;;;;;;;;;;;;;;;
;; fill-table moved to vinzi.eis.bedr.patientRecent.clj, as this namespace remained the only user of function fill-table
;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Auxiliary functions to map a table to a single map by
;;   taking one column as key and another as value.
;;   (and a concrete use-case for viewing knot-tables.)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn map-table-to-hashmap 
  "Take two columns of a table and transform them to a hash-map/lookup-table (each row becoming a separate map-entry).
   The key is based on 'keyCol' and the value is taken from 'valCol'."
  [schema tblNme keyCol valCol]
  ;; Can be used to map a knot-table to a hash-map to allow for convenient
  ;; debugging information
  (let [lpf (str "(map-table-to-hashmap " schema " " tblNme " " keyCol " " valCol)
        qry (str "SELECT " (qs keyCol) ", " (qs valCol)
                 "\n\tFROM " (qsp schema tblNme))]
    (if (table-exists schema tblNme)
      (sql/with-query-results res [qry]
        (let [kk (keyword (str/lower-case keyCol))
              kv (keyword (str/lower-case valCol))
              ks (map kk res)
              vs (map kv res)
              mapping (zipmap ks vs)
              ]
          (doall mapping)))
      (do
        (warn lpf " table does not exists (yet)")
        nil))))

(defn knot-to-hashmap "Extract the knot-table as a hash-map from schema 'vam'."
  [knotName]
  (let [knotId (str/join (conj (vec (take 3 knotName)) "_ID"))]
    (map-table-to-hashmap "vam" knotName knotId knotName)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   functions to obtain information on the columns of a table
;;    (obtained from 'information-schema')
;;


(defn get-col-info 
  "get information on all columns (limited set) ordered by ordinal position.
   (Assumes that the table is located in the current catalog)."
  [schema tblNme]
  (let [catalog (let [conn (sql/connection)]
                  (if (re-find #"\.mysql\." (str conn))
                    "def"     ;; assume mysql data to be in the 'def' catalog.
                    (.getCatalog conn)))
        schema  (strip-dQuotes schema)
        tblNme  (strip-dQuotes tblNme)
        sql (str "SELECT column_name, data_type, ordinal_position "
                 "    , character_maximum_length, numeric_precision "
                 " FROM information_schema.columns "
                 " WHERE table_catalog = " (sqs catalog)
                 "    AND table_schema = " (sqs schema)
                 "    AND table_name = " (sqs tblNme)
                 " ORDER BY ordinal_position ASC;")]
    (trace "(get-col-info): running query: " sql)
    (sql/with-query-results res [sql]
                            (trace "received columns: " (with-out-str (pprint res)))
                            (doall res))))



(defn get-col-type "Get the type of a column."
  [catalog schema tblNme colNme]
  (let [colDefs (get-col-info catalog schema tblNme)
;	_  (do (println colDefs)   (flush))
	colDefs (filter #(= (:column_name %) colNme) colDefs)]
    (trace "looking for colNme: " colNme)
    (trace "filtered colDefs are: " colDefs)
    (assert (= (count colDefs) 1))
    (:data_type (first colDefs))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Materializing views (and manage the corresponding tables.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn materialize-view 
  "Take a view and materialize it as a table and add a primary key (sequence of field-names) and generate new statistics (ANALYZE). 
   Compute colDefs only if not provided. (database might be slow to commit to information_schema)."
  ([schema view table primary-key]
     (let [colDefs (get-col-info schema view)]
       (materialize-view schema view table primary-key colDefs)))
  ([schema view table primary-key colDefs]
    (let [lpf "(vSql/materialize-view): "
          qTbl (qsp schema table)
          create-mat-table (fn []
                             (let [flds (map #(str (:column_name %)
                                                   "  " (:data_type %)) colDefs)]
                               (create-table schema table flds true)))
          materialize-view (fn []
                             (let [fldStr  (str/join ", "
                                                     (map #(qs (str/trim (:column_name %))) colDefs))
                                   qry (str "INSERT INTO " qTbl 
                                            " (" fldStr ") \n"
                                            "SELECT " fldStr "\n"
                                            "FROM " (qsp schema view))]
                               (trace   lpf "qry: " qry)
                               (sql/do-commands qry)))
          add-primaryKey (fn []
                           (when (seq primary-key)
                             (add-primary-key schema table primary-key))) 
          analyze (fn []
                    ;; generate some new statistics
                    (sql/do-commands (str "ANALYZE " qTbl)))]
      ;; main loop
      (if (table-exists schema view)
        (do
          (create-mat-table)
          (materialize-view)
          (add-primaryKey))
        ;; throw exception (no error-msg first)
        (throw (Exception. (str lpf "View " (qsp schema view) " does not exist.")))))))



(defn split-qualified-name 
  "Split a qualified name by returning a map with a schema :schema and a name :table."
  [qNme]
  (let [lpf "(split-qualified-name): "
        qNme (str/trim qNme)]
    (if (= (first qNme) \")
      (let [res (if-let [res (re-find #"\"([\w\s_]*)\"\.\"([\w\s_]*)\"" qNme)]
                  res
                  (re-find #"\"([\w\s_]*)\"" qNme))
            [whole schema tbl] res]
          (if (= (count whole) (count qNme))
            (if tbl
              {:schema schema
               :table  tbl}
              {:schema nil     
               :table  schema})
            (let [msg (str lpf " extracted schema=" schema
                   " and table=" tbl
                   ". This does not cover full string " qNme)]
                 (error msg)
                 (throw (Exception. msg)))))
      (let [msg (str lpf " requires a fully qualified name "
                     " (including double quotes). Received: " qNme)]
        (error msg)
        (throw (Exception. msg))))))





(defn do-commands-safe 
  "Run a command within an existing connection and report back whether there is an exception.
   (exceptions are not reported to the log-file)."
  [qry]
  (try
    (sql/do-commands qry)
    "No exception"
    (catch Exception e
      (println "exception observed: " (.getMessage e))
      (vExcept/except-str "" e))))





(defn pdebug 
  "Print arguments to screen and add a debug-statement."
  [& args]
  (apply println args)
  (debug (apply str args)))

(defn pwarn 
  "Print arguments to screen and add a warn-statement."
  [& args]
  (apply println args)
  (warn (apply str args)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Adds data to table with prependTag in front.
;;   Creates table if is does not exists
;;

(def RowNrLabel "nr")
(def TagLabel   "tag")

;; NOTE/TODO:  this function is also relevant for sql-code. So move it to the cdp package as a helper.
(defn write-tagged-data-to-table
  " If prependTag contains a string the data will be extended with a tag added per row. The table is
   pruned for the tag or is recreated from scratch (if it does not exist)."
  [targetDb dbSchema tblNme colDefs prependTag data]
  (let [lpf "(write-data-to-table): "
	do-rpt (fn []
		 (let [qTbl (qsp dbSchema tblNme)     ;; if no schema given use tablename (schema becomes public. )
		       ;; extend the data if no :nr column present
		       ;; TODO:  forbid string arguments in colDefs?
		       [colDefs hasNums] (let [fcd (first colDefs)]
					   (pdebug "TMP: fcd=" fcd  "  and test " (and (map? fcd) (not= (:nme fcd) RowNrLabel)) )
					   (if (or (and (map? fcd) (not= (:nme fcd) RowNrLabel))
						   (and (string? fcd) (not (or (.startsWith fcd RowNrLabel)
									       (.startsWith fcd (qs RowNrLabel))))))
					     (let [colDefs (cons {:nme RowNrLabel :tpe :integer} colDefs)]
					       (pdebug "TMP: colDefs modified to: " colDefs)
					       [colDefs false])
					     [colDefs true]))
		       
		       colDefs (if prependTag (cons {:nme TagLabel :tpe :text} colDefs) colDefs)
		       tblExists  (table-exists dbSchema tblNme)
		       lastNr  (if  (or (nil? prependTag)
					(not tblExists))
				 (do
				   (pdebug lpf "Create table: " qTbl " with definition: " colDefs)
				   (create-table dbSchema tblNme colDefs true)
				   0)
				 (let [qry (str "DELETE FROM " qTbl " WHERE " (qs TagLabel) " = " (sqs prependTag) ";")
				       lastNrQry (str "SELECT MAX(" RowNrLabel ") AS lst FROM "qTbl ";")
				       _ (pdebug lpf "run query: " lastNrQry)
				       lastNr (sql/with-query-results res [lastNrQry]
						(pdebug lpf " max-row returned: " res
							"\n\t for query: " lastNrQry)
						(if (seq res)
						  (if-let [lst (:lst (first res))] lst 0)
						  0))]
				   (pdebug lpf "Extend/update table: " qTbl " with tag: " prependTag)
				   (sql/do-commands qry)   ;; drop the existing records for this tag
				   ;; lastNr is queried before drop, so newer records always get higher numbers then existing record 
				   ;; (unless a mdx-query drops last recs and does not insert anything)
				   lastNr))
		       _ (pdebug lpf  "TMP: lastNr=" lastNr  "  and count data= " (count data))
		       data   (if (or (> lastNr 0) (not hasNums))
				(if hasNums
				  (map #(if (map? %)
					  (assoc % :nr (+ (:nr %) lastNr))
					  (vec (cons (+ (first %) lastNr) (next %)))) data)  ;; renumber the data
				  (map #(if (map? %1)
					  (assoc %1 :nr %2)
					  (vec (cons %2 %1))) data (range lastNr 10000))) ;; add/prefix number
				data)]
		   ;; perform some reporting
		   (let [sRow (str/join "," (apply str (first data)))
			 size  (count sRow)]
		     (if (> size 6000)
		       (pwarn lpf "String-representation is " size " bytes. Postgresql accepts records of at most 8160 bytes."
			      "\n record might exceed this bound. ")
		       (pdebug "string-representation of first row has size: " (count sRow))))
		   ;; and write the data to the table
		   (if (> (count data) 0)
		     (let [_  (pdebug " prepepndTag=" prependTag " and (first data): " (take 3 data))
			   data (if prependTag
				  (map #(if (map? %)
					  (assoc % :tag prependTag)
					  (vec (cons prependTag %))) data)
				  data)
			   _  (pdebug " prependTag=" prependTag " and (first data): " (take 3 data))
			   data (if (map? (first data))  ;; rewrite maps to vectors
				  (let [ks (map #(keyword (:nme %)) colDefs)]
				    (pdebug lpf " keysequence: " ks " (keys first): " (keys (first data)))
				    (map #(map % ks) data))
				  data)]
		       (apply sql/insert-rows qTbl data))
		     (pwarn lpf "ZERO data rows"))))]
    (if targetDb
      (let [targetDb (if (string? targetDb)
                       (assert false)  ;;(pentConn/find-connection targetDb) ;; map strings to a conneciton.
                       targetDb)]
	(pdebug lpf "writing data to target-database: " (assoc targetDb :password "---"))
	(sql/with-connection targetDb
	  (do-rpt)))
      (do-rpt))))   ;; operate on the existing connection.


(defn get-sql-str-val 
  "This function takes value and returns it in a string-format that can be included in an SQL-string. It does not
   do a type-check on the value (or not yet)."
  ;; TODO: implement other types
  ;; TODO: perform escaping on string values
  [val tpe]
  (let [lpf "(get-sql-str-val): "
        ;; some conversion as you can not use (keyword ...) within a case
        tpe (if (= tpe (keyword "double precision"))
              :double
              (if (= tpe (keyword "character varying"))
                :text
                (if (or (= tpe (keyword "timestamp without time zone"))
                        (= tpe (keyword "timestamp with time zone")))
                  :timestamp
                  tpe)))]
    (case tpe
      (:integer :real :double) (do (println "TMP return plain for type:" tpe "  and val=" val)
                                 (str val))
      (:text :string)  (sqs val)
      (:timestamp :date) (do  (println "TMP return date")
                           (sqs (vDate/generate-sql-date-str val)))
      (vExcept/throw-except lpf " type " tpe " not recognized."))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Auxiliaries to update a database table based on a record set
;;     - update-recs
;;     - add-missing-rows
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn update-recs 
  "Update of a table for a sequence of records. The update-string is generated per record, such that
   the number of updates fields might differ for each record. The appropriate types are retrieved from
   the table-definition. This function does not do a type-check on the provided values."
  [schema tbl idKey recs]
  {:pre [(string? schema) (string? tbl) (keyword idKey) (sequential? recs)]}
  (let [lpf "(update-recs): "
        colDefs (get-col-info schema tbl)
        typemap (zipmap (map (comp keyword :column_name) colDefs)
                        (map (comp keyword :data_type) colDefs))
        get-type (fn [k]
                   (if-let [tp (typemap k)] 
                     tp 
                     (vExcept/throw-except lpf "Key: " idKey " not found in table having keys: " (keys typemap))))
        idTpe (get-type idKey)
        get-kv-str (fn [[k v]]
                     ;;(println "vSQL:TMP-get-kv-str  k="k "  and v= "v)
                     (str (qs (name k)) "=" (get-sql-str-val v (get-type k))))
        fmt (str "UPDATE " (qsp schema tbl) " SET %s WHERE " (qs (name idKey)) " = %s;")
        update (fn [rec]
                 (let [idVal (if-let [id (idKey rec)]
                               id
                               (vExcept/throw-except lpf " id-key: " idKey " missing in record: " rec))
                       idVal (get-sql-str-val idVal idTpe)
                       setStr (->> (dissoc rec idKey)
                                (map get-kv-str )
                                (str/join ", " ))
                       qry (format fmt setStr idVal)]
                   (sql/do-commands qry))) ]
    (doseq [rec recs]
      (update rec))))




(defn add-missing-rows 
    "Add rows from the data-set to the table for which the keyField
    does not exist yet.
    Assumes the key is a single column."
  [schema tbl keyField data]
  {:pre [(keyword? keyField) (sequential? data)]}
  (let [lpf  "(add-missing-rows): " 
        qTbl (qsp schema tbl) 
        kFields (set (map keyField data))
        kQry (str "SELECT " (name keyField) "  FROM " qTbl ";")
        kDb   (sql/with-query-results res [kQry]
                (let [kDb (set (remove nil? (map keyField res)))]
                  (when-not (= (count kDb) (count res))
                    (vExcept/throw-except lpf " table " qTbl 
                      " seems to contains duplicate entries on key: " 
                      keyField))
                  kDb))
        missing (set/difference kFields kDb)]
    (when (seq missing)
      (let [data (zipmap (map keyField data) data)
            add (map data missing)]
        (assert (seq add))
        (apply sql/insert-records qTbl add)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Auxiliary routines to perform operation with defaultDb
;;


(defn defDb-do-commands
  "Execute a 'cmd' for it's side effects on the database.
   If you want to view the output then use 'defDb-show-query-results'."
  [& cmds]
  (println "Use ShowSql if you want to view the output")
  (sql/with-connection defaultDb (apply sql/do-commands cmds)))

(defn defDb-show-query-results "Open the default database, run each of the 'cmds' and print the results."
  [& cmds]
  (letfn [(doShow [cmd]
		   (println "running command: " cmd)
		   (sql/with-query-results res
		     [(str cmd)]
		     (doseq [rec res]
		       (println rec))))
	   ]
    (sql/with-connection defaultDb
      (doseq [cmd cmds]
        (doShow cmd)))))

(defmacro defDb-form "Open the default database and evaluates 'form' in this scope."
  [form]
  `(sql/with-connection defaultDb ~form)) 

(defmacro defDb-function "Open the default database and evaluate 'func' without parameters in this scope."
  [func]
  `(sql/with-connection defaultDb (~func))) 


(defn full-row-transformer 
  "For this table generate a function that extracts all rows in sequence from a record.
   The sql/inser-record function does not work when the columnnames contain dashes or dots (when they represent names of persons)"
  [schema table]
  (let [lbls (->> (get-col-info schema table)
               (sort-by :ordinal_position)
               (map :column_name )
               (map keyword))]
    (fn [rec]
      (map rec lbls))))


(defn chunked-write-data 
  "A chunked write that writes data via a binary partition-process, used to write a dataset contains key-violations.
   By using a binary division strategie it tries to write the data in a limited number of batches
   and reports each of the rows that violate the constraint.
   (When insertion-time makes records unique, it might even be able to resolve a key-violation as 
   different batches get a different inser-time.)"
  [data schema tbl]
  (let [lpf "(chuncked-write-data): " 
        qTbl (qsp schema tbl)
        write-data (fn [data]
                     (when (> (count data) 0)
                       (try
                         (apply sql/insert-records qTbl data)
                         (catch Exception ex
                           (if (= (count data) 1)
                             (error lpf "Failed to write row: " (first data) " to table " qTbl)
                             (chunked-write-data data schema tbl))))))
        cntHead (/ (count data) 2)
        head    (take cntHead data)
        tail    (drop cntHead data)]
    (write-data head)
    (write-data tail)))

(defn chunked-write-select
  "Write output of the 'SelectSql' to table in chunks."
  [selectSql schema tbl]
  (sql/with-query-results data [selectSql]
    (chunked-write-data data schema tbl)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  An SQL sort-order DIFFERS from the java-compare sort order on special characters/
;;  The function below creates a tables with 200 one-character strings and shows the
;;  clojure_sort_order (equal to the byte-value of the character) and 
;;  the sql_sort_order.

(comment
  
  (defn fill-order-test2 [n]
    (let [chars (for [x (range 1 n)] {:nr x :s (str (char x))})
          chars (sort-by :s chars)
          chars (map #(assoc %1 :clojure_sort_order %2) chars (range 1 n))]
    (apply sql/insert-records "order_test" chars)))
  
(defn test-order []
  (let [N 200]
  (sql/with-connection vSql/defaultDb
      (sql/do-commands "DROP TABLE IF EXISTS order_test;")  
      (sql/do-commands "CREATE TABLE order_test (nr INTEGER, s VARCHAR, clojure_sort_order INTEGER, sql_sort_order INTEGER);")
      (fill-order-test2 N)
  
      (sql/with-query-results res ["SELECT * FROM order_test ORDER BY s"]
            (let [res (map #(assoc %1 :sql_sort_order %2) res (range 1 N))]
              (doseq [r res]
                (println r)
                (sql/do-commands (str "UPDATE order_test SET sql_sort_order=" (:sql_sort_order r)
                                      " WHERE s='" (let [c (:s r)] (if (= c "'") "''" c)) "'")))))
      )))

 )  ;; end order test
