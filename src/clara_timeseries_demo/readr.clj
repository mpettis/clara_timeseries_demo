(ns clara-timeseries-demo.readr
  "utilities for reading in a file to a dataframe"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [java-time :as jtime]
            [clojure.set :as set]
            [clara-timeseries-demo.dplyr :as dplyr]))

;;; Helper function
(defn- transpose [m]
  (apply mapv vector m))

;;; Read in a CSV file to a hashmap, a column name is converted to a keyword, the body values are a sequence.
(defn read-csv [filepath]
  (let [csv-parsed (with-open [reader (io/reader filepath)] (doall (csv/read-csv reader)))
        csv-length (- (count csv-parsed) 1)]
    (as-> csv-parsed it
      (transpose it)
      (for [[h & r] it] [(keyword h) r])
      (into {} it))))

(def csv-file
  (read-csv (clojure.string/join "/" [(System/getProperty "user.dir") "dat" "ex01-recommendation.csv"])))
csv-file



;;; Mutate a column.  Replace old column or compute new one.
;;; Not really elegant, but it is my first pass.
(defn mutate [df col val]
  (into df {col val}))
;; (as-> csv-file it
;;   (mutate it :timestamp (map #(jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" %) (:timestamp it)))
;;   (mutate it :units_online (map #(Integer. %) (:units_online it))))



;;; number of rows in a dataframe
(defn nrow [df]
  (let [ks (keys df)]
    (count ((first ks) df))))
;(nrow csv-file)


;; Stack dataframes
(defn bind-rows [df1 df2]
  (->>
    (let [all-keys (set/union (set (keys df1)) (set (keys df2)))]
      (for [col all-keys]
        (let [df1-col (get df1 col (nrow df1))
              df2-col (get df2 col (nrow df2))]
          {col (concat df1-col df2-col)})))
    (into {})))


;(def csv-file
;  (as-> (read-csv (clojure.string/join "/" [(System/getProperty "user.dir") "dat" "ex01-history.csv"])) it
;    (mutate it :timestamp (map #(jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" %) (:timestamp it)))
;    (mutate it :units_online (map #(Integer. %) (:units_online it)))))
;(def csv-file2
;  (as-> (read-csv (clojure.string/join "/" [(System/getProperty "user.dir") "dat" "ex01-recommendation.csv"])) it
;    (mutate it :timestamp (map #(jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" %) (:timestamp it)))
;    (mutate it :units_online (map #(Integer. %) (:units_online it)))))
(bind-rows csv-file csv-file2)

