(ns vinzi.tools.test.vExcept
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vExcept :as vExcept]]))


(deftest vExcept-tests
   (let [ex (Exception. "test1")]
     ;; with report
     (vExcept/clear-lastException)
     (try ;; outer catch
       (try  ;; inner catch
         (is (= @vExcept/lastException nil) "Last exception still nil")
         (throw ex)
         (catch Exception e
           (vExcept/report "inner catch" e)
           (is (vExcept/is-lastException? ex) "lastException should be set now")
           (throw e)))  ;; rethrow
       (catch Exception e
         (vExcept/report "outer catch" e)
           (is (vExcept/is-lastException? ex) "lastException should be set now")))
     (println " Do a visual check of the log-file to discover both messages, but only a single trace.")

     ;; with report-rethrow  and throw
     (try ;; outer catch
       (try  ;; inner catch
         (is (= @vExcept/lastException ex) "Last exception still nil")
         (vExcept/throw-except "TEST-2")
         (catch Exception e
           (is (not= @vExcept/lastException e) "should be a different exception")
           (vExcept/report-rethrow "inner catch-2" e) ;; rethrow
           (is false "Should never be reached, due to rethrow.")))  
       (catch Exception e
         (vExcept/report "outer catch-2" e)
           (is (not (vExcept/is-lastException? ex)) "lastException should be a different exception now.")))
         ))

