(ns vinzi.tools.vDbInspect
(:use   [clojure 
         [pprint :only [pprint pp]]]
        [clojure 
         [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools 
         [logging :only [error info trace debug warn]]]
        [vinzi.tools.vSql :only [qs qsp sqs]])
(:require [clojure
           [string :as str]
           [set :as set]]
            [clojure.java 
             [io :as io]
             [jdbc :as sql]]
            [clojure.data [xml :as dXml]]
            [vinzi.tools
             [vCsv :as vCsv]
             [vFile :as vFile]
             [vSql :as vSql]
             [vXml :as vXml]])
)





(defn generate-table-xml
  [schema tblNme]
  (let [cols (vSql/get-col-info schema tblNme)
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
                      (map #(assoc % :name (str/lower-case (:name %))) )
                      (map #(dXml/element :field % nil) ))
        fields (dXml/element :fields {} colDescr)]
    (dXml/element :table
             {:name tblNme}
             (list fields))))


(defn get-tables [schema]
  (let [qry (str "SELECT table_name FROM information_schema.tables "
                 "WHERE table_schema = " (sqs schema) ";")]
    (sql/with-query-results res [qry]
      (doall (map :table_name res)))))


(defn get-schema-description
   "Get an xml-definition of 'schema' using the connection 'db'."
   [db schema]
  (sql/with-connection db
    (let [tblDef (->> schema
                     (get-tables )
                     (map (partial generate-table-xml schema) )
                     (doall ))]
      (dXml/element :csv-extraction
                    {}
                    tblDef))))


(defn write-schema-description
  "Write a schema-description of 'schema' 
   to an xml-file with name 'fName', using the connection
   defined by 'db'."

  [fName db schema]
  (spit fName (dXml/indent-str (get-schema-description db schema))))

   
