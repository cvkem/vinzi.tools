(ns vinzi.tools.vDbAnalize
(:use   [clojure 
         [pprint :only [pprint pp]]]
        [clojure 
         [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools 
         [logging :only [error info trace debug warn]]]
        [vinzi.tools.vSql :only [qs qsp sqs]])
(:require [clojure
           [set :as set]
           [string :as str]]
          [clojure.java 
             [io :as io]
             [jdbc :as sql]] 
         [vinzi.tools 
             [vDbInspect :as vdbi]
             [vExcept :as vExcept]
             [vFile :as vFile]
             [vSql :as vSql]]))


(defn min-max-avg [qTbl name]
  (let [qry (str " SELECT MIN(" name ") AS min "
                    "\n\t, MAX(" name ") AS max "
                    "\n\t, AVG(" name ") AS avg "
                    "\n\t, SUM(CASE WHEN " name " IS NULL"
                           " THEN 1 ELSE 0 END) AS num_null"
                    "\n\tFROM " qTbl ";")]
    (println "execute: " qry)
    (sql/with-query-results res [qry]
      (first res))))

(defn min-max-avg-len [qTbl name]
  (let [qry (str " SELECT MIN(LENGTH(" name ")) AS min "
                    "\n\t, MAX(LENGTH(" name ")) AS max "
                    "\n\t, AVG(LENGTH(" name ")) AS avg "
                    "\n\t, SUM(CASE WHEN " name " IS NULL "
                             " THEN 1 ELSE 0 END) AS num_null"
                    "\n\tFROM " qTbl ";")]
    (println "execute: " qry)
    (sql/with-query-results res [qry]
      (first res))))

(defn enumerate [qTbl name groupBy]
  (let [groupBy (if (seq groupBy)
                  (str/replace groupBy #"%f%" name)
                  name)
        qry (str " SELECT " groupBy " AS " name 
                    "\n\t, count(*) AS num "
                    "\n\tFROM " qTbl 
                    "\n\t  GROUP BY " groupBy
                    "\n\t  ORDER BY " groupBy ";")]
    (println "execute: " qry)
    (sql/with-query-results res [qry]
      (doall res))))


(defn report-table
  "Analyze a table and return the result as a 
   sequence of hashmaps."
  [{:keys [name schema type colDescr] :as tDescr}]
  (let [lpf "(report-table): "
        qTbl (qsp schema name)
        report-column
          (fn [{:keys [name data_type validate groupBy] :as cDescr}]
            (let [qName (qs name)
                  res (case validate
                        "min-max-avg" (min-max-avg qTbl qName)
                        "min-max-avg-len" (min-max-avg-len qTbl qName)
                        "enumerate" (enumerate qTbl qName groupBy)
                        (vExcept/throw-except lpf "validation " 
                            validate " is not known"))]
              {:name name
               :data_type data_type
               :analysis res}))]
    (assoc (dissoc tDescr :colDescr)
        :numRows (vSql/table-length schema name) 
        :analysis (doall (map report-column colDescr)))))


(defn analyse-db
  "Analyseer een database aan de hand van de omschrijving
   uit file 'fNmame' (edn-file aangemaakt via vinzi.tools.vDbInspect)."
  [db fName]
  (let [descr (vdbi/read-validate-description fName)
        report (fn []
                 (sql/with-connection db
                   (doall (map report-table descr))))
        prt (fn [x] (with-out-str (pprint x)))]
    (spit (str fName ".report") (prt (report)))))

