(ns vinzi.tools.test.test
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vFile :as vFile]
     [vMap :as vMap]]))


(deftest vFile-expansion
  (let [currDir (vFile/get-current-dir)]
    (println "Running tests regarding file-expansion")
    (are [x y] (= (vFile/get-path-dir x) y)
         "test"          currDir
         "test/"          currDir
         "dir/test"      (str currDir vFile/FileSep "dir")
         "/dir/test"     "/dir"
         "./test"        currDir
         "./dir/test"    (str currDir vFile/FileSep "dir")
         "./dir/test/"    (str currDir vFile/FileSep "dir"))
        ))
