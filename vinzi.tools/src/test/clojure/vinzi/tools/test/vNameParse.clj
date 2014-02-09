(ns vinzi.tools.test.vNameParse
  (:use clojure.test)
  (:require
    [clojure.string :as str]
    [vinzi.tools
     [vDateTime :as vDate]
     [vNameParse :as vNP]
     [vMap  :as vMap]]))

(deftest name-parse
  (are [inp outp] (vMap/map-compare (vNP/add-initials-abbrev (vNP/split-name inp)) outp)
       "jan van abc"  {:firstnames "jan" :infix "van" :lastnames "abc" :initials "J." :abbrev "JvA"}
       "Jan van abc"  {:firstnames "Jan" :infix "van" :lastnames "abc" :initials "J." :abbrev "JvA"}
       "Jan van Abc"  {:firstnames "Jan" :infix "van" :lastnames "Abc" :initials "J." :abbrev "JvA"}
       "jan a"  {:firstnames "jan" :infix "" :lastnames "a" :initials "J."  :abbrev "JA"}
       "Jan van de Abc"  {:firstnames "Jan" :infix "van de" :lastnames "Abc" :initials "J." :abbrev "JvdA"}
       "Jan Van De Abc"  {:firstnames "Jan" :infix "Van De" :lastnames "Abc" :initials "J." :abbrev "JvdA"}
       "Jan in 't Abc"  {:firstnames "Jan" :infix "in 't" :lastnames "Abc" :initials "J." :abbrev "JiA"}
       "Jan Piet in 't Abc"  {:firstnames "Jan Piet" :infix "in 't" :lastnames "Abc" :initials "J.P." :abbrev "JPiA"}
       "Jan Piet Abc"  {:firstnames "Jan Piet" :infix "" :lastnames "Abc" :initials "J.P." :abbrev "JPA"}
       "Jan Van De Abc-Def"  {:firstnames "Jan" :infix "Van De" :lastnames "Abc-Def" :initials "J." :abbrev "JvdAD"}
       "Jan Van De Abc Def"  {:firstnames "Jan" :infix "Van De" :lastnames "Abc Def" :initials "J." :abbrev "JvdAD"}
       ;; when no space and thus no infix a space is assumed after last dot to allow a split in firstnames and lastnames
       "J.abc"  {:firstnames "J." :infix "" :lastnames "abc" :initials "J." :abbrev "JA"}
       "J.P.abc"  {:firstnames "J.P." :infix "" :lastnames "abc" :initials "J.P." :abbrev "JPA"}
       )
  )


