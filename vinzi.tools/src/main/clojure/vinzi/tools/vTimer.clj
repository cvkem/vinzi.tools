(ns vinzi.tools.vTimer
  (:use	[clojure.tools 
         [logging :only [error info trace debug warn]]])
  (:require [clojure 
             [string :as str]]))


(defn get-elapsedSec-timer []
  (let [start (System/currentTimeMillis)]
    ;; times shows millis sinds 1970 (not the run-time)
    (fn [] (let [elapsed (/ (- (System/currentTimeMillis) start) 1000.0)]
	     elapsed))))



(defn gen-phase-timer [phaseKey]
  (let [timer (get-elapsedSec-timer)
	timings (atom {phaseKey (double 0.0)})
	lastPhase (atom phaseKey)
	lastTime (atom (timer))]
    (letfn [(status* []
	      @timings)
	    (switch-phase [newKey]
	       (let [nt (timer)
		     lti @lastTime
		     ti (- nt lti)
		     lf  @lastPhase]
		 (swap! lastTime (fn [_] nt))
		 (swap! timings (fn [timings]
				  (let [lt (get timings lf)
					timings (assoc timings
						  lf (+ lt ti))
					timings (if (get timings newKey)
						  timings
						  (assoc timings 
						    newKey (double 0.0)))]
				    timings)))
		 (swap! lastPhase (fn [_] newKey))
		 (status*)))
	    (status []
	      ;; fake-switch to update the statistics of the current phase
	      (switch-phase @lastPhase)
	      (status*))
	    (report []
		    (str/join "\n" (sort (status))))]
      {:switch-phase  switch-phase
       :status       status
       :report report})))



    (defn test-phase-timer []
      (let [{:keys [switch-phase report]} (gen-phase-timer :a)]
	(switch-phase :b)
	(println (report))
	(Thread/sleep 100)
	(switch-phase :c)
	(println (report))
	(Thread/sleep 100)
	(switch-phase :a)
	(println (report))
  ))