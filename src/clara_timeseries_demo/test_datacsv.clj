(ns clara-timeseries-demo.test-datacsv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [java-time :as jtime]))


;;; This will read in the CSV file.  It is a seq of vectors, row-oriented.
(with-open [reader (io/reader (clojure.string/join "/" [(System/getProperty "user.dir") "dat" "ex01-recommendation.csv"]))]
  (doall
    (csv/read-csv reader)))


;;; Useful manipulations

(def csv-parsed
  (with-open [reader (io/reader (clojure.string/join "/" [(System/getProperty "user.dir") "dat" "ex01-recommendation.csv"]))]
    (doall
      (csv/read-csv reader))))


;; Get the header
(first csv-parsed)

;; Get the body
(rest csv-parsed)

;; Get a column based on header
(defn csv-col [csv-in col-name]
  "Get column of data based on header"
  (let [col-index (.indexOf (first csv-in) col-name)]
    (map #(% col-index) (rest csv-in))))

(csv-col csv-parsed "timestamp")
(csv-col csv-parsed "units_online")


;; Transpose the CSV file
;; https://stackoverflow.com/questions/10347315/matrix-transposition-in-clojure
(defn transpose [m]
  (apply mapv vector m))

(transpose csv-parsed)



;; Parse timestamp
;; http://dm3.github.io/clojure.java-time/README.html
(rest (csv-col csv-parsed "timestamp"))

;; Format gotten here: https://stackoverflow.com/questions/36136041/clojure-clj-time-parse-local-string
(map #(jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" %)
     (rest (csv-col csv-parsed "timestamp")))

;; See also:
;; https://cljdoc.org/d/tupelo/tupelo/0.9.182/api/tupelo.java-time
;; http://dm3.github.io/clojure.java-time/java-time.html#var-local-date-time
;; https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
;; http://dm3.github.io/clojure.java-time/java-time.html#var-formatter
;(jtime/local-date-time "yyyy-MM-dd HH:mm:ss" "2020-07-01T15:15:00")
;(jtime/local-date-time :iso-local-date-time "2020-07-01T15:15:00")
;(jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" "2020-07-01T15:15:00")



;; Durations
(def start-time
  (jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" "2020-07-01T15:15:00"))
(def end-time
  (jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" "2020-07-01T16:45:00"))

;; start time first is a positive time, reversing them gives negative time
(jtime/duration start-time end-time)
(jtime/duration end-time start-time)


;; http://dm3.github.io/clojure.java-time/java-time.html#var-as
(jtime/as (jtime/duration start-time end-time) :minutes)



;; Sort datetimes
(sort
  [ (jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" "2020-07-01T15:15:00")
   (jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" "2020-07-01T13:15:00")])



