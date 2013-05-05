(ns vinzi.tools.test.vSql
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vSql :as vSql]]))


(deftest vSql-tests
  ;; testing qs
  (are [x res] (= (vSql/qs x) res)
       "test"        "\"test\""
       "\"test\""    "\"test\"")
  ;; unbalanced quotes throw exception
  (are [x res] (thrown? Exception (vSql/qs x))
       "\"test"    
       "test\"")
  
  ;; testing sqs
  (are [x res] (= (vSql/sqs x) res)
       "test"        "'test'"
       "'test'"    "'test'")
  ;; unbalanced quotes throw exception
  (are [x res] (thrown? Exception (vSql/sqs x))
       "'test"    
       "test'")
  
  ;; testing strip-dQuotes
  (are [x res] (= (vSql/strip-dQuotes x) res)
       "\"test\""        "test"
       "test"            "test")
  ;; unbalanced quotes throw exception
  (are [x res] (thrown? Exception (vSql/strip-dQuotes x))
       "\"test"    
       "test\"")
  
  )



(deftest test_split-qualified-name
  (are [q schema tbl] (= (map (vSql/split-qualified-name q) [:schema :table]) '(schema tbl))
       (vSql/qsp "sch" "tbl")   "sch"      "tbl"
       "\"schema\".\"table\""   "schema"   "table"
       "\"sch_ema\".\"table\""   "sch_ema"   "table"
       "\"schema\".\"t_able\""   "schema"   "t_able"
       ;; new version excepts names without schema, as long as they are quoted.
       (vSql/qs "table")        nil        "table")
  ;; New version of split throws exceptions on errors in the input.
  (are [x] (thrown? Exception (vSql/split-qualified-name x))
       nil
       "noquoted.name"
       "\"unbalanced_quote"))
  
(deftest gendb
  (is (= (vSql/generate-db {:db-name "hibernate" :user "user" :password "password"})
         {:classname "org.postgresql.Driver", :subprotocol "postgresql", :subname "//localhost/hibernate", :user "user", :password "password"})
      " Failure when parsing a user/password/db-name")
  (is (= (vSql/generate-db {:url "hsqldb:hsql://192.158.56.1/hibernate" "hsqldb:hsql:" "192.158.56.1" :user "user" :password "password"} )
         {:classname "org.postgresql.Driver", :subprotocol "hsqldb:hsql", :subname "//192.158.56.1/hibernate", :user "user", :password "password"})
      " Failure when parsing a url parameter without a port")
  (is (= (vSql/generate-db {:url "hsqldb:hsql://192.158.56.1:9001/hibernate" "hsqldb:hsql:" "192.158.56.1" :user "user" :password "password"})
         {:classname "org.postgresql.Driver", :subprotocol "hsqldb:hsql", :subname "//192.158.56.1:9001/hibernate", :user "user", :password "password"})
      " Failure when parsing a url parameter with a port")
  (is (= (vSql/generate-db {:url "jdbc:hsqldb:hsql://192.158.56.1:9001/hibernate" "hsqldb:hsql:" "192.158.56.1" :user "user" :password "password"})
         {:classname "org.postgresql.Driver", :subprotocol "hsqldb:hsql", :subname "//192.158.56.1:9001/hibernate", :user "user", :password "password"})
      " Failure to remove the 'jdbc:' prefix")
  )  
(comment ;; old version using map-compare
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

  ) ;; end comment