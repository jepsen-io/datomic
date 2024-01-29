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
            [jepsen.datomic [checker :as datomic.checker]
                            [db :as db]
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
  [[:stats]
   [:stats, :partition]
   [:stats, :partition-storage]
   [:stats, :kill]
   [:stats, :pause]
   [:stats, :clock]
   [:stats, :kill, :pause, :clock, :partition, :partition-storage]])

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all [:stats, :pause :kill :partition :partition-storage :clock]})

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
                         :stable-period (:nemesis-stable-period opts)
                         :interval (:nemesis-interval opts)})]
    (merge tests/noop-test
           opts
           {:name (str (name workload-name)
                       (when (:sync opts) " sync")
                       " " (str/join "," (map name (:nemesis opts))))
            :os os
            :db db
            :plot    {:nemeses (:perf nemesis)}
            :checker (checker/compose
                       {:perf  (checker/perf)
                        :clock (checker/clock-plot)
                        :stats (checker/stats)
                        :datomic (datomic.checker/stats-checker)
                        :exceptions (checker/unhandled-exceptions)
                        :workload (:checker workload)
                        })
            :client (:client workload)
            :nemesis (:nemesis nemesis nemesis/noop)
            :generator (->> (:generator workload)
                            (gen/stagger (/ (:rate opts)))
                            (gen/nemesis (:generator nemesis))
                            (gen/time-limit (:time-limit opts)))})))

(def cli-opts
  "Command-line option specification"
  [[nil "--aws-transactor-role ROLE" "If provided, uses an existing AWS transactor role for Dynamo."]

   [nil "--aws-peer-role ROLE" "If provided, uses an existing AWS peer role for Dynamo."]

   [nil "--clean-peer" "If set, recompiles the peer code instead of using a cached copy."]

   ["-c" "--consistency-model MODEL" "If specified, overrides the checker and verifies the given consistency model holds over the entire history."
    :parse-fn keyword]

   [nil "--dynamo-read-capacity UNITS" "How many units of read capacity to give DynamoDB."
    :default 250
    :parse-fn parse-long]

   [nil "--dynamo-table NAME" "If given, uses an existing Dynamo table. You must also specify --aws-transactor-role and --aws-peer-role. The table will not be created, provisioned, or deleted by Jepsen; the caller is responsible for doing so. This will break `test-all` and `--test-count`, both of which run multiple tests back-to-back and expect a fresh state each time."]

   [nil "--dynamo-write-capacity UNITS" "How many units of read capacity to give DynamoDB."
    :default 500
    :parse-fn parse-long]

   [nil "--key-count NUM" "Number of keys in active rotation."
    ; Note: we go super large because we want to very occasionally affect keys
    ; that have fallen into cold storage tiers, even over tests of a billion
    ; txns.
    :default  25
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  32
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--memory-index-threshold SIZE" "Datomic's memory index threshold, for transactors."
    :default "16m"
    :validate [#(re-find #"^\d+[kmg]$") "should be something like 2m or 15g"]]

   [nil "--memory-index-max SIZE" "Datomic's memory index maximum, for transactors."
    :default "32m"
    :validate [#(re-find #"^\d+[kmg]$") "should be something like 2m or 15g"]]

   [nil "--min-txn-length NUM" "Minumum number of operations in a transaction."
    :default  1
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause
                                 :kill
                                 :partition
                                 :partition-storage
                                 :clock
                                 :stats})
               "Faults must be pause, kill, partition, partition-storage, clock, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default  10
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--nemesis-stable-period SECS" "If given, rotates the mixture of nemesis faults over time with roughly this period."
    :default nil
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   [nil "--object-cache-max SIZE" "Maximum size of the object cache, for both transactors and peers."
    :default "64m"
    :validate [#(re-find #"^\d+[kmg]$") "should be something like 2m or 15g"]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-s" "--sync" "Issue a `sync` call before any read-only queries"
    :default false]

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
    (for [n     nemeses
          w     workloads
          sync  [true false]
          i     (range (:test-count opts))]
      (datomic-test (assoc opts
                           :nemesis n
                           :workload w
                           :sync sync)))))

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
