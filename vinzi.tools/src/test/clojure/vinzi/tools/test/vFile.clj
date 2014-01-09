(ns vinzi.tools.test.vFile
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [clojure.java.io :as io]
    [vinzi.tools.vFile :as vFile]))


(deftest test-path-type
  (are [path res] (= (vFile/absolute-path? path) res)
       "/path"  true
       "/path/dir"  true
       "./path"  false
       "./path/dir"  false
       "../path"  false
       "../path/dir"  false
       "~/path" false
       "~/path/dir" false
       "path" false
       "path/dir" false
       )
  (are [path res] (= (vFile/explicit-relative-path? path) res)
       "./path"  true
       "./path/dir"  true
       "../path"  false
       "../path/dir"  false
       "path" false
       "path/dir" false
       "/path" false
       "/path/dir" false
       "~/path" false
       "~/path/dir" false
       )
  (are [path res] (= (vFile/parent-path? path) res)
       "../path"  true
       "../path/dir"  true
       "./path"  false
       "./path/dir"  false
       "path" false
       "path/dir" false
       "/path" false
       "/path/dir" false
       "~/path" false
       "~/path/dir" false
       )
  (are [path res] (= (vFile/home-path? path) res)
       "~/path"  true
       "~/path/dir"  true
       "../path"  false
       "../path/dir"  false
       "path" false
       "path/dir" false
       "/path" false
       "/path/dir" false
       "./path" false
       "./path/dir" false
       )
  (are [path res] (= (vFile/full-path? path) res)
       "~/path"  true
       "~/path/dir"  true
       "/path" true
       "/path/dir" true
       "./path" true
       "./path/dir" true
       "../path"  false       ;;  parent-paths are no full-paths (need a base to be extended.
       "../path/dir"  false
       "path" false
       "path/dir" false
       )
  )

(deftest test-filename
  (are [base fName res] (= (vFile/filename base fName) res)
       "/base"  "test"  "/base/test"
       ;; trailing slashes are correctly processed
       "/base"  "test/"  "/base/test/"
       "/base/"  "test/"  "/base/test/"
       "/base/"  "test/"  "/base/test/"
       ;; spaces will be retained (and not escaped)
       "/base"  " test"  "/base/ test"
       "/base"  "test "  "/base/test "
       "/base"  "test it"  "/base/test it"
       ;; absolute paths will not be extended
       "/base"  "/test"  "/test"
       ;; fName can contain subpaths
       "/base"  "dir/test"  "/base/dir/test"
       ;; ~ expansion to home-dir (overrides base)
       "/base"  "~/test"  (vFile/filename (get (System/getenv) "HOME") "test")
       "~"  "test"  (vFile/filename (get (System/getenv) "HOME") "test")
       "~/"  "test"  (vFile/filename (get (System/getenv) "HOME") "test")
       " ~"  "test"  (vFile/filename (get (System/getenv) "HOME") "test")
       "~/ "  "test"  (vFile/filename (get (System/getenv) "HOME") "test")
       ;; however, no space allowed between ~ and /
       "~ /"  "test"  "~ /test"
       ;; moving to parent folders
       "/folder1/folder2/" "../file"   "/folder1/file"
       nil "./ble" "./ble"
       nil  "~/ble" (str (get (System/getenv) "HOME") "/ble")
;       "/folder1/folder2/" "../../file"   "/file"
;       "/folder1/folder2/" "../"   "/folder1"
       )

       ;;;using more than two arguments!!
       (is (= (vFile/filename "a" "b" "c.clj") "a/b/c.clj"))
       (is (= (vFile/filename "/a" "b" "c.clj") "/a/b/c.clj"))
       (is (= (vFile/filename "a" "/b" "c.clj") "/b/c.clj"))
       (is (= (vFile/filename "a" "./b" "c.clj") "./b/c.clj"))
       (is (= (vFile/filename "a" "~/b" "c.clj") 
              (str (get (System/getenv) "HOME") "/b/c.clj")))      
       (is (= (vFile/filename "a" "b" "c" "d.clj") "a/b/c/d.clj"))
       (is (= (vFile/filename "a" "b" "c" "./d.clj") "./d.clj"))
       (is (= (vFile/filename "a" "b" "./c" "d.clj") "./c/d.clj"))
       (is (= (vFile/filename "a" "./b" "c" "d.clj") "./b/c/d.clj"))
       (is (= (vFile/filename "a" "b" "." "c" "d.clj") "./c/d.clj"))
       (is (= (vFile/filename "a" "b" "./" "c" "d.clj") "./c/d.clj"))
       (is (= (vFile/filename "a" "./b" "c" "d.clj") "./b/c/d.clj"))
       )

(deftest test-strip-last-folder
  (are [path res]  (= (vFile/strip-last-folder path) res)
       "/path/folder" "/path/"
       "/folder" "/"
       )
  ;; do not return empty path-strings as these don't have a clearly defined semantics (root or current-dir)
  (is (thrown? Exception (vFile/strip-last-folder "folder")))
  )

(deftest test-strip-parent-prefix
  (are [path res] (= (vFile/strip-parent-prefix path) res)
       "../path" "path"
       "../../path" "../path"
       "../" ""
       ".." ""
       )
  (are [path] (thrown? Exception (vFile/strip-parent-prefix path))
       "path"
       "path/file"
       ""
  ))


(deftest test-get-path-dir
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


(deftest test-filepath-parts
  (are [f split] (= (vFile/filepath-parts f) split)
       "2012" '("2012")
       "data/2012" '("data" "2012")
       (io/file "data/2012")  '("data" "2012")
       "/data/2012" '("" "data" "2012")
       (io/file "/data/2012")  '("" "data" "2012")
       (io/file "data/2012")  '("data" "2012")
       ;; ~ currently is not expanded 
       ))


(deftest test-change-file-base
  (are [targ skipNum srcNme res] (= (vFile/change-file-base srcNme skipNum targ) res)
       ;; skipping 0 returns an absolute path
       "/TST" 0 "/d1/d2/file" "/d1/d2/file"
       "/TST" 0 "d1/d2/file" "/TST/d1/d2/file"
       "/TST/" 0 "d1/d2/file" "/TST/d1/d2/file"
       "/TST" 1 "/d1/d2/file" "/TST/d1/d2/file"
       "/TST/" 1 "/d1/d2/file" "/TST/d1/d2/file"
       "TST" 1 "/d1/d2/file" "TST/d1/d2/file"
       "/TST/" 2 "/d1/d2/file" "/TST/d2/file"
       "TST" 2 "/d1/d2/file" "TST/d2/file"
       ))

(deftest get-relative-path
  (are [base path res] (= (vFile/get-relative-path base path) res)
       "/tmp/"  "/tmp/ble" "ble"
       "/tmp"  "/tmp/ble" "ble"
       "/tmp/"  "/tmp/ble/blap" "ble/blap"
       "/tmp"  "/tmp/ble/blap" "ble/blap"
       "/tmp/"  "/tmp/ble" "ble"
       ;; Next test should check agains $HOME
 ;;      "/home/cees" "~/ble" "ble"
       )
  (let [currDir (vFile/get-current-dir)
        currDir (if (= (last currDir) \.)
                  (apply str (drop-last currDir))
                  currDir)]
  (are [base path res] (= (vFile/get-relative-path base path) res)
        currDir  (vFile/filename currDir "ble") "ble"
        currDir  (vFile/filename "." "ble") "ble"
        currDir  (str "." vFile/FileSep "ble") "ble"
       )
    (are [path res] (= (vFile/get-relative-path path) res)
       (vFile/filename currDir "ble")  "ble"))
  )

