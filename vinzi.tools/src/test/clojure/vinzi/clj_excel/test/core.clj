(ns vinzi.clj-excel.test.core
  (:use [vinzi.clj-excel.core])
  (:use [clojure.test]))

(def testFileName "/tmp/newfile_45879276.xls")

  

(defn write-test-file [fName]
  (let [tstData2 {
               :double 1.23
;;               :float  (float 4.5) ;; float is not accepted.
               :string "test-string"
               :pseudo "<span class=\"pseudof\" key=\"ABC\"/>"
               :java_date  (java.util.Date.)
               :sql_date   (java.sql.Date. (.getTime (java.util.Date.)))
    ;;           :int        (int 1)
               :long       (long -10)
               }
        tstRows2 (list (keys tstData2) (vals tstData2))
        nb (workbook)] 
    (merge-rows nb 1 tstRows2)
    (save nb fName)))
  
(deftest course-grain-test 
  (let [f (java.io.File. testFileName)]
    (when (.exists f)
      (.delete f))
    (is (not (.exists f)) "file shouldn't exists before start of test")
    (write-test-file testFileName)
    (is (.exists f) "if file does not exist some part of the code failed (weak test as it does not test content of file)")
  ))
