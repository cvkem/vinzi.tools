(ns vinzi.tools.test.vCsv
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools.vCsv :as vCsv]))


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
