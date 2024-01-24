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
                             (let [attrs (:attrs value)]
                               ;(info :attrs attrs)
                               {:time      (util/nanos->secs time)
                                :datoms    (:datoms value)
                                :keys      (:count (:append/key attrs))
                                :txInstant (:count (:db/txInstant attrs))})))
                    (t/into [])
                    (h/tesser history))
        series [:datoms :keys :txInstant]
        _ (info "Have" (count points) "points")
        ;points (take 5 points)
        ]
    ; Pull apart into different series
    (zipmap series
            (mapv (fn [series]
                    (->> points
                         (filter series)
                         (mapv (juxt :time series))))
                  series))))

(defn stats-checker
  "A checker that emits a plot of stats."
  []
  (reify checker/Checker
    (check [this test history opts]
      (when (seq history)
        (let [datasets (stats-datasets history)
              ;_ (info (with-out-str (pprint datasets)))
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
            ;(info "Plotting to" output-path)
            (-> plot
                (perf/without-empty-series)
                (perf/with-range)
                (perf/with-nemeses history (:nemeses (:plot test)))
                (perf/plot!))
            ;(info "Plot complete"))))
      {:valid? true})))
