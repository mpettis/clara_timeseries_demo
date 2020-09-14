(ns clara-timeseries-demo.thrashing
  "Test detection of thrashing."
  (:require 
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.pprint :refer [pprint]]
    [clara.rules :as r]
    [incanter.core :as i]
    [incanter.io :as iio]
    [java-time :as jtime]
    ))


;; Load csv file into incanter dataframe data structure.
(def df
  (with-open [reader (io/reader (clojure.string/join "/" [(System/getProperty "user.dir") "dat" "thrashing-01.csv"]))]
    (doall
      (as-> reader it
        (iio/read-dataset it :header true)
        (i/transform-col it :units_online #(Integer. %))
        ;(i/transform-col it :timestamp #(jtime/local-date-time "yyyy-MM-dd HH:mm:ssX" %))
        (i/transform-col it :timestamp #(jtime/local-date-time "yyyy-MM-dd'T'HH:mm:ss" %))
        ))))

;; Show base datastructure
(pprint (into {} df))

;; Select subset of df -- here, its columns
;(i/sel df :cols [:timestamp :src :units_online])
(i/sel df :cols [:timestamp :src])


;; Calculate thrashing
;;
;; Need to do this based on sequence of units_online in time, which is what is in `df`.
;;
;; Rule: If proposed change is 3rd change in last 5 observations, it is thrashing.
(i/$ :units_online df)
(map-indexed vector (i/$ :units_online df))
(map-indexed (fn [idx itm] [idx itm]) (i/$ :units_online df))

;; https://stackoverflow.com/questions/1427894/sliding-window-over-seq
(partition 5 1 (i/$ :units_online df))

(for [w (partition 5 1 (i/$ :units_online df))]
  w)

;; This gets a sliding window of values to compute on.  Is same length as
;; original series.  Window truncates to have fewer elements when on left side
;; of sequence.
(for [i (range (count (i/$ :units_online df)))]
  (let [lb (max 0 (- i 5))
        irange (range lb (inc i))
        window-vals (mapv #(get (vec (i/$ :units_online df)) %) irange)
        deltas (mapv - window-vals (rest window-vals))]
    (cond
      (< (count deltas) 4)                                   false
      (and (>= (count (filter #(not= 0 %) deltas)) 3)
           (not= (last deltas) 0)
           (= "recommendation" (get (vec (i/$ :src df)) i))) true
      :else false)
    ))


