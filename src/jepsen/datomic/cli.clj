(ns jepsen.datomic.cli
  "Command-line entry point for Datomic tests"
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [os :as os]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.datomic [db :as db]
                            [nemesis :as dn]]
            [jepsen.datomic.workload [append :as append]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps"
  {:append append/workload
   :none   (constantly tests/noop-test)})

(def all-workloads
  "A collection of workloads we run by default"
  [:append])

(def all-nemeses
  "Combinations of nemeses we run by default."
  [[]
   [:partition-storage]
   [:kill]
   [:pause]
   [:clock]
   [:kill, :pause, :clock, :partition-storage]])

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all [:pause :kill :partition-storage :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(defn datomic-test
  "Takes CLI options and constructs a Jepsen test map"
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db opts)
        os            debian/os
        nemesis       (dn/nemesis-package
                        {:db db
                         :nodes (:nodes opts)
                         :faults (:nemesis opts)
                         :partition {:targets [:one :majority]}
                         :pause {:targets [:one :majority :all]}
                         :kill {:targets [:one :majority :all]}
                         :interval (:nemesis-interval opts)})]
    (merge tests/noop-test
           opts
           {:name (str (name workload-name)
                       " " (str/join "," (map name (:nemesis opts))))
            :os os
            :db db
            :checker (checker/compose
                                      {:perf (checker/perf {:nemeses (:perf nemesis)})
                                       :clock (checker/clock-plot)
                                       :stats (checker/stats)
                                       :exceptions (checker/unhandled-exceptions)
                                       :workload (:checker workload)})
            :client (:client workload)
            :nemesis (:nemesis nemesis nemesis/noop)
            :generator (->> (:generator workload)
                            (gen/stagger (/ (:rate opts)))
                            (gen/nemesis (:generator nemesis))
                            (gen/time-limit (:time-limit opts)))})))

(def cli-opts
  "Command-line option specification"
  [[nil "--clean-peer" "If set, recompiles the peer code instead of using a cached copy."]

   ["-c" "--consistency-model MODEL" "What consistency model should we check for?"
    :default :strong-session-serializable
    :parse-fn keyword]

   [nil "--dynamo-read-capacity UNITS" "How many units of read capacity to give DynamoDB."
    :default 250
    :parse-fn parse-long]

   [nil "--dynamo-write-capacity UNITS" "How many units of read capacity to give DynamoDB."
    :default 250
    :parse-fn parse-long]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  16
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition-storage :clock})
               "Faults must be pause, kill, partition-storage, clock, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default  10
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-v" "--version STRING" "What version of Datomic should we install?"
    :default "1.0.7075"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :default  :append
    :missing  (str "Must specify a workload: " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] all-workloads)]
    (for [n nemeses, w workloads, i (range (:test-count opts))]
      (datomic-test (assoc opts :nemesis n :workload w)))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  datomic-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
