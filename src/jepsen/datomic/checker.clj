(ns jepsen.datomic.checker
  "Extra Datomic-specific checkers"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]
                    [store :as store]]
            [jepsen.checker.perf :as perf]
            [gnuplot.core :as g]
            [tesser.core :as t])
  (:import (jepsen.history Op)))

(defn stats-datasets
  "Takes a history and produces a map of metrics (e.g. :datoms to [time,
  value] pairs."
  [history]
  (let [points (->> (t/filter
                      (fn [^Op op]
                        (and (identical? :nemesis (.process op))
                             (identical? :stats (.f op))
                             (.value op))))
                    (t/map (fn [{:keys [time value]}]
                             {:time   (util/nanos->secs time)
                              :datoms (:datoms value)
                              :keys   (:append/key value)
                              :txInstant (:txInstant value)}))
                    (t/into [])
                    (h/tesser history))
        series [:datoms :keys :txInstant]]
    ; Pull apart into different series
    (zipmap series
            (mapv (fn [series]
                    (mapv (juxt :time series) points))
                  series))))

(defn stats-checker
  "A checker that emits a plot of stats."
  []
  (reify checker/Checker
    (check [this test history opts]
      (when (seq history)
        (let [datasets (stats-datasets history)
              output-path (.getCanonicalPath
                            (store/path! test
                                         (:subdirectory opts)
                                         "datomic-stats.png"))
              plot {:preamble
                    (concat (perf/preamble output-path)
                            [[:set :title (str (:name test)) " datomic stats"]])
                    :series (map (fn [[series points]]
                                   {:title (name series)
                                    :with :lines
                                    :data points})
                                 (sort datasets))}]
          (when (perf/has-data? plot)
            (-> plot
                (perf/without-empty-series)
                (perf/with-range)
                (perf/with-nemeses history (:nemeses (:plot test)))
                (perf/plot!)))))
      {:valid? true})))
