(ns vinzi.tools.test.vCsv
  (:use clojure.test)
  (:use clojure.test
        [clojure.pprint :only [pprint pp]]
        [vinzi.tools.vSql :only [qsp qs]])
  (:require [clojure.string :as str]
            [clojure.java
             [io :as io]
             [jdbc :as sql]]
            [vinzi.tools
             [vSql :as vSql]
             [vMap :as vMap]]
    [vinzi.tools.vCsv :as vCsv]))



(def testCsvFile1 "/tmp/cdp_tools_test1.csv")
(def testCsvFile2 "/tmp/cdp_tools_test2.csv")

(def testCsvResult '({:c1 "1" :c2 "a"}
                     {:c1 "2" :c2 "b"}))

(def testCsvResultConvert '({:c1 1 :c2 "a"}
                             {:c1 2 :c2 "b"}))

(def testCsvData1 "c1,  c2
  1,a
  2 ,  b")

(def testCsvData2 "c1;  c2
  1;a
  2 ;  b")



(use-fixtures :once (fn [f]
                      (println "Init ONCE fixture")
                      (spit testCsvFile1 testCsvData1)
                      (spit testCsvFile2 testCsvData2)
                      (sql/with-connection vSql/defaultDb
                                          (f))
                      (println "Clean-up ONCE fixture")
  ;;                    (io/delete-file1 testCsvData1 true)
                      ))


(deftest csvTest
  (println "running csv tests")
  (is (= (vCsv/read-csv-map testCsvFile1 ) testCsvResult)
      "Reading file with , as separator and converting it to a map")
  (is (= (vCsv/read-csv-map testCsvFile2 :separator \;) testCsvResult)
      "Reading file with ; as separator and converting it to a map")

  ;; now testing read-csv  (the version used in cdp-files.
  (let [testPar1 {:csvFile testCsvFile1}
        testPar2 {:csvFile testCsvFile2
                  :separator \; }]
    (is (= (vCsv/read-csv testPar1) testCsvResult)
        "Reading file via params with , as separator and converting it to a map")
    (is (= (vCsv/read-csv testPar2) testCsvResult)
        "Reading file via params with ; as separator and converting it to a map"))
    
  ;; (def ...) Does not work in test environment as
  ;;  1. an unbound var is defined during compilation
  ;;  2. def in form only works if this root-binding exits.
  ;;  Use dynamic compilation to prevent this (via eval)
  (if (resolve 'extend-path)
    (println "can not test extend-path")
    (do
      (println "GOING to define 'extend-path")
      ;; using eval instead of def
      (eval (list 'def 'extend-path (fn [x] (str "GLOBAL_EXTEND/" x))))
      (println "extend-path in TEST resolved to: " (ns-resolve *ns* 'extend-path))
      (is (= (vCsv/extend-csv-path "test") "GLOBAL_EXTEND/test")
          "The extend-path from this namespace should be called.")
      (println "Ran test, now unmap symbol extend-path")
      (ns-unmap *ns* 'extend-path)))
  
  
  )

(deftest csvTest-db
  ;; create an empty test-table
  (let [table "csv_test_table"
        qTable (qsp "public" table)
        drop (str "DROP TABLE IF EXISTS " qTable ";")
        trunc (str "DELETE FROM " qTable ";")
        qry (str "CREATE TABLE " qTable "(c1 Varchar(10), c2 VARCHAR(10)); ")]
    (sql/do-commands drop)
    (sql/do-commands qry)
    
    (let [params {:targetTbl table
                  :schema "public"
                  :csvFile testCsvFile1}]
      (vCsv/read-csv-to-db params)
      (let [qry (str "SELECT * "
                     "\nFROM " qTable 
                     "\nORDER BY c1, c2 ASC; ")]
        (sql/with-query-results res [qry]
                                (is (vMap/compare-map-arrays res testCsvResult)
                                    "De inhoud van de database-tabel is anders dan verwacht.")))
      
      (vCsv/read-csv-to-db params)
      (let [qry (str "SELECT * "
                     "\nFROM " qTable 
                     "\nORDER BY c1, c2 ASC; ")]
        (sql/with-query-results res [qry]
;;                                (pprint res)
                                (is (vMap/compare-map-arrays res (interleave testCsvResult testCsvResult))
                                    "Reading a second time should add all rows again (duplicating rows).")))
      (sql/do-commands trunc)
      (let [params (dissoc params :schema)]
        (println "Now running without :schema being specified. (assumes the default-schema is 'public'.")

        (vCsv/read-csv-to-db params)
        (let [qry (str "SELECT * "
                       "\nFROM " qTable 
                       "\nORDER BY c1, c2 ASC; ")]
          (sql/with-query-results res [qry]
                                  (is (vMap/compare-map-arrays res testCsvResult)
                                      "De inhoud van de database-tabel is anders dan verwacht.")))
        
        )
      )))


(deftest csvTest-db-map-convert
  ;; create an empty test-table
  (println "Running the same test, but now a automatic type-conversion from string to integer is performed.")
  (let [table "csv_test_table"
        qTable (qsp "public" table)
        drop (str "DROP TABLE IF EXISTS " qTable ";")
        trunc (str "DELETE FROM " qTable ";")
        qry (str "CREATE TABLE " qTable "(c1 integer, c2 VARCHAR(10)); ")]
    (sql/do-commands drop)
    (sql/do-commands qry)
    
    (let [params {:targetTbl table
                  :schema "public"
                  :csvFile testCsvFile1}]
      (vCsv/read-csv-to-db params)
      (let [qry (str "SELECT * "
                     "\nFROM " qTable 
                     "\nORDER BY c1, c2 ASC; ")]
        (sql/with-query-results res [qry]
                                (is (vMap/compare-map-arrays res testCsvResultConvert)
                                    "De inhoud van de database-tabel is anders dan verwacht.")))
      
      (vCsv/read-csv-to-db params)
      (let [qry (str "SELECT * "
                     "\nFROM " qTable 
                     "\nORDER BY c1, c2 ASC; ")]
        (sql/with-query-results res [qry]
;;                                (pprint res)
                                (is (vMap/compare-map-arrays res (interleave testCsvResultConvert testCsvResultConvert))
                                    "Reading a second time should add all rows again (duplicating rows).")))
      (sql/do-commands trunc)
      (let [params (dissoc params :schema)]
        (println "Now running without :schema being specified. (assumes the default-schema is 'public'.")

        (vCsv/read-csv-to-db params)
        (let [qry (str "SELECT * "
                       "\nFROM " qTable 
                       "\nORDER BY c1, c2 ASC; ")]
          (sql/with-query-results res [qry]
                                  (is (vMap/compare-map-arrays res testCsvResultConvert)
                                      "De inhoud van de database-tabel is anders dan verwacht.")))
        
        )
      )))



;;;;;;;;;;;;;;;;
;;  mapTest


(deftest test-map-cols
  (let [data [["a" "b" "c"] [1 2 3] [4 5 6]]
        columnMap {"b" "NewB"}
        columnMap2 {:b "NewB"}
        columnMap3 {:b :NewB}
        keepColRes '(["a" "NewB" "c"] [1 2 3] [4 5 6])
        keepColResKw '([:a :NewB :c] [1 2 3] [4 5 6])
        dropColRes '(["NewB"] [2] [5])
        dropColResKw '([:NewB] [2] [5])
        ]
    (are [keepCols keywordizeKeys colMap outp] (= (vCsv/csv-columnMap data colMap keepCols keywordizeKeys) outp)
         false false columnMap dropColRes
         true  false columnMap keepColRes
         false false columnMap2 dropColRes
         true  false columnMap2 keepColRes
         false false columnMap3 dropColRes
         true  false columnMap3 keepColRes
         false true columnMap dropColResKw
         true  true columnMap keepColResKw
         false true columnMap2 dropColResKw
         true  true columnMap2 keepColResKw
         false true columnMap3 dropColResKw
         true  true columnMap3 keepColResKw
         )
    ;; default for keepcols true
    (is (= (vCsv/csv-columnMap data columnMap) keepColRes))
    ))
