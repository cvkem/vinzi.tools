(ns vinzi.tools.test.test
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vFile :as vFile]
     [vMap :as vMap]
     [vSql :as vSql]]))


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


(deftest gendb
  (is (vMap/map-compare (vSql/generate-db {:db-name "hibernate" :user "user" :password "password"})
         {:classname "org.postgresql.Driver", :subprotocol "postgresql", :subname "//localhost/hibernate", :user "user", :password "password"})
      " Failure when parsing a user/password/db-name")
  (is (vMap/map-compare (vSql/generate-db {:url "hsqldb:hsql://192.158.56.1/hibernate" "hsqldb:hsql:" "192.158.56.1" :user "user" :password "password"} )
         {:classname "org.postgresql.Driver", :subprotocol "hsqldb:hsql", :subname "//192.158.56.1/hibernate", :user "user", :password "password"})
      " Failure when parsing a url parameter without a port")
  (is (vMap/map-compare (vSql/generate-db {:url "hsqldb:hsql://192.158.56.1:9001/hibernate" "hsqldb:hsql:" "192.158.56.1" :user "user" :password "password"})
         {:classname "org.postgresql.Driver", :subprotocol "hsqldb:hsql", :subname "//192.158.56.1:9001/hibernate", :user "user", :password "password"})
      " Failure when parsing a url parameter with a port")
  (is (vMap/map-compare (vSql/generate-db {:url "jdbc:hsqldb:hsql://192.158.56.1:9001/hibernate" "hsqldb:hsql:" "192.158.56.1" :user "user" :password "password"})
         {:classname "org.postgresql.Driver", :subprotocol "hsqldb:hsql", :subname "//192.158.56.1:9001/hibernate", :user "user", :password "password"})
      " Failure to remove the 'jdbc:' prefix")
  )
