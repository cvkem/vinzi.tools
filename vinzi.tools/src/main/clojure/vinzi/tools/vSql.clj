(ns vinzi.tools.vSql
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as sql]
            [vinzi.tools.vExcept :as vExcept]))



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
   (including one step higher in the exception-chain."
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
  (str (qs schema) "." (qs nme)))



(defn sqs "Change s to a single-quoted sting,
   unless it is already single-quoted (no trimming of spaces on argument)."
  [s]
  (let [s (str s)]
    (if (seq s)
      (let [startQ (= (first s) \')
            endQ   (= (last s) \')]
        (if (or startQ endQ)
          (if (and startQ endQ)
            s    ;; string is single quoted already
            (throw (Exception. (str "(vSql/qs) unbalanced quotes in argument: " s ))))
          (str "'" s "'")))
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

(defn db-key "Translate a fieldname to a string that retrieves the field from the jdbc hashmaps." [x]
  (keyword (str/lower-case x)))

(defn generate-db [{:keys [classname subprotocol
                          db-host db-port db-name
                          user password]}]
  (when (not db-name)
    (error "Definition of db-name is manditory in generate-db"))
  (when (not user)
    (error "Definition of user is manditory in generate-db"))
  (when (not password)
    (error "Definition of password is manditory in generate-db"))

  {:classname (if classname classname (:classname defaultDb))
          :subprotocol (if subprotocol subprotocol (:subprotocol defaultDb))
          :subname (str "//" (if db-host db-host "localhost")
                             (if db-port (str ":" db-port) "")
                             "/" db-name)
          :user user
          :password password})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  auxiliary functions for testing purpose
;;  (shortened version of vinzi.tools.sqlTools/table-exists, to prevent dependency)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn table-exists "Check wether a 'table' exists in 'schema' (using current db-connection)"
  ;; the alternative would be to inspect pg_tables (postgres specific)
  [schema table]
  (let [schema (strip-dQuotes schema)
        table  (strip-dQuotes table)
        qry (format "select * from information_schema.tables WHERE table_name LIKE '%s' AND table_schema = '%s';" table schema)]
     (sql/with-query-results res [qry]
            (let [outcome (if (seq res)
                            true
                            false)]
              outcome))))

(defn table-length [schema table]
  (let [qry (str "SELECT count(*) AS cnt FROM " (qsp schema table) ";")]
    (sql/with-query-results res [qry]
                            (assert (= (count res) 1))
                            (:cnt (first res)))))




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




(defn get-create-field-str "Return fieldsnames selected by 'sel' as quotes strings 
  followed by the rest of the string (type, default-values, etc..) 
  and interposed by commas(for usage in a create statement)."
  [flds sel]
  (let [lpf "(get-create-field-str): "
        fldNames (map #(nth flds %) sel)
        _ (trace lpf "selected fldNames: " fldNames)
        fldNames (letfn [(quote-first [org]
                                      (let [org (str/trim org)]
                                        (if (= (first org) \")
                                          org  ;; return unmodified
                                          (let [s (str/split org #"\s+")
                                                head (qs (first s))
                                                tail (str/join " " (rest s))]
                                            (str head "\t" tail)))))]
                        (map quote-first fldNames))
        _ (trace "after quoting fldNames: " fldNames)
        fldNames  (interpose ", " fldNames)]
    (apply str fldNames)))

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



(defn create-table "Create a table 'qTblName' with fields 'fields' if it does not exist yet. 
  If 'dropIt' is true than it tries to drop the table first via a drop CASCADE!
  The 'fields'-parameter is a list of field definition strings (including attributes)."
  [schema tblNme fields dropIt]
  (let [lpf "(create-table): "
        qTblNme (qsp schema tblNme)
        flds (get-create-field-str fields (range (count fields)))
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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Function to fill a table based on a field-definition
;;  Can be used to generate denormalized tables, where each
;;  column has it's own sql-query.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn fill-table "fill the table 'schema'.'tblNme' using the query defined by 'fldDef'.
The 'fldDef' is a vector containing a hash-map per table-item with keys
  - 'fld':  field in the target table,
  - 'type': type of the fields
  - 'src' : name of the matching field from the alias.
  - 'def' : def of the query that retrieves the fields.
All queries are LEFT JOIN-ed on the primary key of the target-table. 
(see patientRecent.clj for an example of usage.)"
  [schema tblNme fldDef]
  (letfn [(create-left-join [qIdField alias matching code]
             (if code
               (let [aliasId (qsp alias matching)
                     select  (str "(" code ") AS " alias)]
                 (str " LEFT JOIN " select " ON " qIdField " = " aliasId "\n"))
               ""))  ;; no code, so return the empty string
          ]
    (let [lpf "(vSql/fill-table): "
          ;; head is the first field. Treated separately
          head  (first fldDef)
          idField (:fld head)
          srcCode (str/trim (:def head))
          srcAlias "keytbl"
          qIdField (qsp srcAlias idField)
          ;; process the rest (will become the left-joins)
          fldDef (rest fldDef)
          src   (map :src fldDef)
          tar   (map :fld fldDef)
          code  (map :def fldDef)
          matching (map #(if-let [m (:matching %)] m idField)  fldDef)
          alias (map #(str (str/lower-case %1) "__" (str/lower-case %2))
                     src tar)
          tar   (map qs tar)
          srcs  (apply str (interpose ", " (cons qIdField src)))
          tars  (apply str (interpose ", " (cons (qs idField) tar)))
          ljoins (map #(create-left-join  qIdField %1 %2 %3)
                      alias matching code)
          ljoins (apply str ljoins)
          qry  (str "  INSERT INTO " (qsp schema tblNme) "(" tars ")\n"
                    "  SELECT " srcs "\n"
                    "  FROM " srcCode " AS " srcAlias "\n"
                    ljoins ";")
          ]
      (debug "Executing query: " qry)
      (println "\n\nExecuting query (println): " qry "\n\n")
      (sql/do-commands qry)
      )))


(defn get-create-fields-fldDef 
  "Extract the fields definition for the create-statement from the 'fldDef'. 
   (used to create a table that later will be filled via (fill-table), see above."
  [fldDef]
  (map #(str (:fld %) "  " (:type %)) fldDef))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  Auxiliary functions to map a table to a single map by
;;   taking one column as key and another as value.
;;   (and a concrete use-case for viewing knot-tables.)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn map-table-to-hashmap 
  "Take two columns of a table and transform them to a hash-map (each row becoming a separate map-entry).
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
              mapping (apply hash-map (interleave ks vs))
              ;; identical and better would be
              ; mapping (zipmap ks vs)
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


(defn get-col-info "get information on all columns (limited set)
   (Assumes that the table is located in the current catalog)."
  [schema tblNme]
  (let [catalog (.getCatalog (sql/connection))
        schema  (strip-dQuotes schema)
        tblNme  (strip-dQuotes tblNme)
        sql (str "SELECT column_name, data_type, ordinal_position "
                 " FROM information_schema.columns "
                 " WHERE table_catalog = " (sqs catalog)
                 "    AND table_schema = " (sqs schema)
                 "    AND table_name = " (sqs tblNme))]
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
  "Take a view and materialize it as a table and add a primary key (sequence of field-names). 
   Compute colDefs only if not provided. (database might be slow to commit to information_schema)."
  ([schema view table primary-key]
     (let [colDefs (get-col-info schema view)]
       (materialize-view schema view table primary-key colDefs)))
  ([schema view table primary-key colDefs]
    (let [lpf "(vSql/materialize-view): "]
      (letfn [(create-mat-table []
                                (let [flds (map #(str (:column_name %)
                                                      "  " (:data_type %)) colDefs)]
                                  (create-table schema table flds true)))
              (materialize-view []
                                (let [fldStr  (str/join ", "
                                                  (map #(qs (str/trim (:column_name %))) colDefs))
                                      qry (str "INSERT INTO " (qsp schema table)
                                               " (" fldStr ") \n"
                                               "SELECT " fldStr "\n"
                                               "FROM " (qsp schema view))]
                                  (trace   lpf "qry: " qry)
                                  (sql/do-commands qry)))
              (add-primaryKey []
                              (when (seq primary-key)
                                (add-primary-key schema table primary-key))) ]
             ;; main loop
             (if (table-exists schema view)
                (do
                  (create-mat-table)
                  (materialize-view)
                  (add-primaryKey))
                ;; throw exception (no error-msg first)
                (throw (Exception. (str lpf "View " (qsp schema view) " does not exist."))))))))



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

(defmacro defDb-function "Open the default database and evaluate 'cmd' in this scope."
  [cmd]
  `(sql/with-connection defaultDb ~cmd)) 


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