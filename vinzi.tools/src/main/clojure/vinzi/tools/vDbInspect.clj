(ns vinzi.tools.vDbInspect
(:use   [clojure 
         [pprint :only [pprint pp]]]
        [clojure 
         [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools 
         [logging :only [error info trace debug warn]]]
        [vinzi.tools.vSql :only [qs qsp sqs]])
(:require [clojure
           [edn :as edn]
           [set :as set]
           [string :as str]]
            [clojure.java 
             [io :as io]
             [jdbc :as sql]]
            [clojure.data [xml :as dXml]]
            [vinzi.tools
             [vCsv :as vCsv]
             [vExcept :as vExcept]
             [vFile :as vFile]
             [vSql :as vSql]
             [vXml :as vXml]])
)

(def  fix-name (atom identity))

(defn force-lower-case-name 
  "Can be used for mysql. Forces field-names to be presented in lower-case" 
  []
  (reset! fix-name str/lower-case))

(defn add-colDescr
  [tblDef]
  (let [{:keys [name schema]} tblDef
        cols (vSql/get-col-info schema name)
        remove-nil (fn [theKey] 
                      #(if (nil? (theKey %))
                              (dissoc % theKey) 
                              %) )
        colDescr (->> cols
                      (map #(set/rename-keys % {:column_name :name
                               :column_type :type
                               :character_maximum_length :num_char
                               :numeric_precision :precision
                                                }) )
                      (map (remove-nil :num_char) )
                      (map (remove-nil :precision) )
                      ;; mysql translate to lower-case
                      (map #(assoc % :name (@fix-name (:name %))) )
                      )]
     (assoc tblDef :colDescr colDescr)))


(defn generate-table-xml
  "Transform a table definition to an xml-description."
  [tblDef]
  (let [{:keys [colDescr]} tblDef
        tblAttrs (dissoc tblDef :colDescr)
        fields (->> colDescr 
                    (map #(dXml/element :field % nil) )
                    (dXml/element :fields {} ))]
    (dXml/element :table tblAttrs fields)))


(defn get-table-defs 
  "Get the table definitions of all tables and 'views' in schema.
   (based on information_schema.tables)."
  [schema]
  (let [qry (str "SELECT table_name, table_schema, table_type "
                 " FROM information_schema.tables "
                 " WHERE table_schema = " (sqs schema) ";")
        get-table-descr (fn [{:keys [table_name table_schema table_type]}]
                          {:name    table_name
                           :schema  table_schema
                           :type   (case table_type
                                     "BASE TABLE" "T"
                                     "VIEW"       "V"
                                     table_type)})]
    (sql/with-query-results res [qry]
      (doall (map get-table-descr res)))))

(defn get-tables-names 
  "Get the list of all tables and views in 'schema'."
  [schema]
      (doall (map :name (get-table-defs schema))))


(defn get-schema-descriptor
   "Get an xml-definition of the csv-extraction of 'schema' using
    the connection 'db'.
    This schema services as the description of an extraction-task
    for the vinzi.extractcsv extractor."
   [db schema tblExtender]
  (sql/with-connection db
    (let [tblDef (->> schema
                     (get-table-defs )
                     (map add-colDescr )
                     (map tblExtender )
                     (doall ))]
      tblDef)))


(defn get-xml-description
   "Get an xml-definition of the csv-extraction of 'schema' using
    the connection 'db'.
    This schema services as the description of an extraction-task
    for the vinzi.extractcsv extractor."
   [db schema mainTag tblExtender]
  (sql/with-connection db
    (let [tblDef (->> (get-schema-descriptor db schema tblExtender)
                     (map generate-table-xml )
                     (doall ))]
      (dXml/element mainTag 
                    {}
                    tblDef))))


(defn add-validation-rules 
  "Add validation description to the fields. The validation description
   is based on the type of the field, and consists of a key :validate
   and and optional key :groupBy."
  [{:keys [colDescr] :as tblDescr}]
  (let [lpf "(add-validation-rules): " 
        extend-column 
         (fn [fld]
           (let [tpe (:data_type fld)
                 _ (when-not (seq tpe) (vExcept/throw-except lpf 
                     " field-descr " fld " does not have a data_type." ))
                 tpe (str/lower-case tpe)
                 vld (cond
                       (or (= tpe "integer") 
                           (= tpe "smallint")
                           (= tpe "bigint")) {:validate "min-max-avg"}
                       (= tpe "numeric") {:validate "min-max-avg"}
                       (= tpe "double precision") {:validate "min-max-avg"}
                       (.startsWith tpe "date") {:validate "enumerate"
                                                 :groupBy "EXTRACT(YEAR FROM %f%) || '-' || EXTRACT(MONTH FROM %f%)"}
                       (.startsWith tpe "timestamp") {:validate "enumerate"
                                                 :groupBy "EXTRACT(YEAR FROM %f%) || '-' || EXTRACT(MONTH FROM %f%)"}
                       (or (.startsWith tpe "varchar")
                           (.startsWith tpe "character varying")
                           (.startsWith tpe "char")
                           (= tpe "text"))   {:validate "min-max-avg-len"}
                       (.startsWith tpe "bool") {:validate "enumerate"}
                       :else {:validate "unknown"}
                       )]
              (into fld vld))) ]
  (assoc tblDescr :colDescr (map extend-column colDescr))))

(defn add-field-description 
  "To be implementated: add a description to the field based on 
   foreign key relations (and possibly use the naming-conventions of
   the anchormodel to elaborate on things.)"
  [{:keys [colDescr] :as tblDescr}]
  (let [add-col-descr (fn [fld]
                        (assoc fld :description ""))]
    (assoc tblDescr :colDescr (map add-col-descr colDescr))))

(defn get-csv-extract-description
   "Get an xml-definition of the csv-extraction of 'schema' usingi
    the connection 'db'.
    This schema services as the description of an extraction-task
    for the vinzi.extractcsv extractor."
   [db schema]
   (get-xml-description db schema :csv-extraction identity))

(defn write-csv-extract-description
  "Write a schema-description of 'schema' 
   to an xml-file with name 'fName', using the connection
   defined by 'db'."

  [fName db schema]
  (spit fName (dXml/indent-str (get-csv-extract-description db schema))))



 (defn get-validate-description
   "Get an xml-definition of the validation step for all tables
    in 'schema' when using the connection 'db'.
    This schema can be customized using a text-editor and will
    subsequently be used to feed vinzi.tools.vDbAnalyze."
   [db schema]
   (let [extend-colDescr (comp add-validation-rules 
                               add-field-description)]
     (get-xml-description db schema :db-validation extend-colDescr)))

(defn write-validate-xml-description
  "Write a schema-description of 'schema' 
   to an xml-file with name 'fName', using the connection
   defined by 'db'."
  [fName db schema]
  (spit fName (dXml/indent-str (get-validate-description db schema))))

(defn write-validate-description
  "Write a schema-description of 'schema' 
   to an xml-file with name 'fName', using the connection
   defined by 'db'."
  [fName db schema]
  (let [extend-colDescr (comp add-validation-rules 
                              add-field-description)
        prt (fn [x] (with-out-str (pprint x))) ]
    (spit fName (prt (get-schema-descriptor db schema extend-colDescr)))))
   
(defn read-validate-description
  [fName]
  (let [lpf "(read-validate-description): "]
    (when-not (.endsWith fName ".edn")
      (warn lpf (str "Expecting edn, but file '" fName 
                   "' does not have .edn extentions")))
    (edn/read-string (slurp fName))))


;;(def validateXmlDescr 
;;   {:db-validation 
;;     {:table {:idAttr :name
;;              :keyMap {:fields  
;;                        {:keyMap {:field {:idAttr :name}}}}}}}
;;)

