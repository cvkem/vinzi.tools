(ns vinzi.tools.test.vStateMachine
  (:use clojure.test)
;  (:use	[clojure 
;         [pprint :only [pprint pp]]]
;        [clojure 
;         [stacktrace :only [print-stack-trace root-cause]]]
;        [clojure.tools 
;         [logging :only [error info trace debug warn]]])
  (:require [clojure.string :as str]
            ;;[clojure.java.shell :as sh]
            [vinzi.tools
             [vStateMachine :as vState]
             [vExcept :as vExcept]]))
  

;;  see vinzi.tools.test.vStatemachine for an example and the input format.


(def output (atom *out*))

(defn print-state [msg]
  (binding [*out* @output]
    (println (str "Msg:" msg))))   ;; additional str to avoid inserted spaces by println


(def stateDescr [{:state :start
                  :init (partial print-state "started")
                  :nextStates #{:phase1 :altPhase}
                  :clean nil}
                 {:state :phase1
                  :init (partial print-state "phase1")
                  :nextStates #{:end}
                  :clean nil}
                 {:state :altPhase
                  :init (partial print-state "altPhase")
                  :nextStates #{:end}
                  :clean (partial print-state "clean-up altPhase")}
                 {:state :end
                  :init (partial print-state "end")
                  } 
                 ])


(deftest test-stateMachine
  (are [inp expect] (= (with-open [out (java.io.StringWriter.)]
                         (swap! output (fn [_] out))
                         ;;           (binding [*in* (java.io.BufferedReader (java.io.StringReader. "start\nphase1\nend\n"))]
                         (with-in-str inp
                           (vState/run-state-machine stateDescr)
                           (let [result (str (.getBuffer out))]
                             (println "obtained: " result)
                             result)))
                       expect)
       "phase1\nend\n"     "Msg:started\nMsg:phase1\nMsg:end\n"
       "altPhase\nend\n"     "Msg:started\nMsg:altPhase\nMsg:clean-up altPhase\nMsg:end\n"    ;; includes a clean operation too.
  ))
             
