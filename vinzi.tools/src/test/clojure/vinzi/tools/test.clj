(ns vinzi.tools.test
  (:use clojure.test)
  (:require [vinzi.tools.vFile :as vFile]))


(deftest vFile-expansion
  (let [currDir (vFile/get-current-dir)]
    (println "Running tests regarding file-expansion")
    (are [x y] (= (vFile/get-path-dir x) y)
         "test"          currDir
         "dir/test"      (str currDir vFile/FileSep "dir")
         "/dir/test"     "/dir"
         "./test"        currDir
         "./dir/test"    (str currDir vFile/FileSep "dir"))
        ))