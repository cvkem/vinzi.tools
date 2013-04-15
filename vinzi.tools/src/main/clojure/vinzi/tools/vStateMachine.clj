(ns vinzi.tools.vStateMachine
  (:use	[clojure 
         [pprint :only [pprint pp]]]
        [clojure 
         [stacktrace :only [print-stack-trace root-cause]]]
        [clojure.tools 
         [logging :only [error info trace debug warn]]])
  (:require [clojure.string :as str]
            ;;[clojure.java.shell :as sh]
            [vinzi.tools 
             [vExcept :as vExcept]]))
  

;;  see vinzi.tools.test.vStatemachine for an example and the input format.



(defn run-state-machine
  "Run thought the state-machine defines by stateDescr."
  [stateDescr]
  (let [lpf "(run-state-machine): "
        states (into {} (map #(vector (:state %) %) stateDescr))]
    (loop [state :start]
      (if (states state)
        (let [{:keys [init nextStates clean]} (states state)]
          (when init (init))
          (when (seq nextStates)
            (let [newState (-> (read-line)
                             (str/trim)
                             (keyword))]
              (println " observed state-transition to:  " newState)
            (when (not (nextStates newState))
              (vExcept/throw-except lpf "state: " newState " is not valid. Allowed transitions are: " nextStates))
            (when clean (clean)) 
            (recur newState))))
        (vExcept/throw-except lpf "State: " state "  was not defined in the stateDescr: " (with-out-str (pprint stateDescr)))))))
      
