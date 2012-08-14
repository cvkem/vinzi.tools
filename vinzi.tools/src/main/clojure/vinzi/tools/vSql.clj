(ns vinzi.tools.vSql
  (:use	[clojure [pprint :only [pprint pp]]]
        [clojure [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools [logging :only [error info trace debug warn]]])
  (:require [clojure
             [string :as str]]
            [clojure.java
             [jdbc :as sql]]))



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
  (error (with-out-str
           (println "Exception of type: " (class e))
           (print-stack-trace (root-cause e))
           (println "Exception caught at location " location)
           (println "Message: " (.getMessage e))
           (when (isa? (class e) java.sql.SQLException)
             (println "SQL-related Exception details:")
             (println "ErrorCode: " (.getErrorCode e))
             (println "SQLState:  " (.getSQLState e))
             (when-let [n (.getNextException e)]
               (println "Next-message: " (.getMessage n))
               (println "Next-errorcode: " (.getErrorCode n)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   functions to produce (double-)quoted strings for Postgres.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn qs
  "Surround a string by double quotes (postgres requires this)"
  [s]
  (str "\"" s "\""))

(defn qsp
  "Create a double-qouted string pair for schema.table or table.field."
  [s t]
  (str (qs s) "." (qs t)))



(defn sqs "Change s to a single-quoted string." [s]
  (str "'" s "'"))

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
  ;; (a more comprehensive function including logging is found in vinzi.tools.sqlTools
  [schema table]
  (let [qry (format "select * from information_schema.tables WHERE table_name LIKE '%s' AND table_schema = '%s';" table schema)]
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


(defn create-sequence "Create a sequence only if it does not exist." [schema seqNme]
  (let [lpf "(create-sequence): "
        qName (qsp schema seqNme)]
    (if (not (sequence-exists schema seqNme))
      (let [qry (str "CREATE SEQUENCE " qName)]
        (trace lpf " running query: " qry)
        (sql/do-commands qry))
      (debug lpf " sequence " qName " exists already (not created)"))))


(defn get-create-field-str "Return fieldsnames selected by 'sel' as quotes strings 
  followed by the rest of the string and interposed by commas(for usage in a create statement)."
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

    (if (table-exists qTblNme)
      (debug lpf "Table: " qTblNme " exists already (abort create proces)")
      (do
        (debug lpf "creating table" qTblNme)
        (trace lpf "create query: " qry)
        (sql/do-commands qry)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;   Functions to drop tables and views and to clear a complete schema.
;;

(defn do-commit "Do a commit on the current connection." []
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
  (let [qry (str "SELECT table_type, table_schema, table_name "
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
   Schema might also be a mask (using %)." [schema]
  (let [qry (str "SELECT sequence_schema, sequence_name "
                 " FROM information_schema.sequences "
                 " WHERE sequence_schema LIKE '" schema "';")]
    (sql/with-query-results res [qry]
      (doseq [item res]
          (drop-it "SEQUENCE" (:sequence_schema item) (:sequence_name item))))))

(defn drop-routines 
  "Remove all routines of schema via a CASCADED DROP. 
   Schema might also be a mask (using %)." 
  [schema]
  (let [qry (str "SELECT routine_schema, routine_name "
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





