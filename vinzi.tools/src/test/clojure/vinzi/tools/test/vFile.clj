(ns vinzi.tools.test.vFile
  (:use clojure.test)
  (:require
    [clojure.string :as str]
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
;       "/folder1/folder2/" "../../file"   "/file"
;       "/folder1/folder2/" "../"   "/folder1"
       
       ))

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


