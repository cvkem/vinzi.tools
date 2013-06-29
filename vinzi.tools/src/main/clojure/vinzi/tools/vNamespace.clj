(ns vinzi.tools.vNamespace
  (:use [clojure.tools 
        [logging :only [info error debug warn trace]]]))
  

(def ns-stack (atom ()))

(defn clear-stack "Clear the whole stack"
  []
  (swap! ns-stack (fn[_] ())))


(defn push-ns "Push the current name-space on the stack."
  [] 
  (let [lpf "(push-ns): "
        theNs (ns-name *ns*)] 
;;    (debug lpf "pushing namespace" theNs) 
    (swap! ns-stack
          (fn [oldstack] (conj oldstack theNs)))))

(defn peek-ns "Peek at the first namespace at the stack, but don't pop it.
  Throw an exception if the stack is empty."
  []
  (let [lpf "(peek-ns): "
        stack @ns-stack]
    (if (seq stack)
      (first stack)  ;; return the value on top of the stack
      (let [msg (str lpf "ns-stack is empty")]
        (error msg)
        (throw (Exception. msg))))))
 
(defn pop-ns "Pop a namespace from the stack and return it."
  []
  (let [lpf "(pop-ns): "
        popNs (peek-ns)]
        (swap! ns-stack (fn [stack] (rest stack)))
        popNs))

(defn switch-ns "Push the current namespace on the stack
  and switch to the namespace 'targetNs'"
  [targetNs]
  (push-ns)
  (debug "(switch-ns): pushed " *ns* " and now move to " targetNs)
  (in-ns targetNs))


(defn return-ns "Pop a namespace from the stack 
  and return to this namespace"
  []
  (when-let [targetNs (pop-ns)]
    (let [fromNs   *ns*]
      (in-ns targetNs)
      (debug "(return-ns): switched from namespace " fromNs " to " *ns*))))

        