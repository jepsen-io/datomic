(ns jepsen.datomic.nemesis
  "Fault injection for Datomic"
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [cheshire.core :as json]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [nemesis :as n]
                    [generator :as gen]
                    [net :as net]
                    [util :as util]]
            [jepsen.datomic [client :as client]
                            [core :as core]
                            [db :as db]]
            [jepsen.nemesis.combined :as nc]
            [org.httpkit.client :as http]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn dynamo-ip-ranges*
  "Scans through DynamoDB's IP ranges and builds a set of them."
  []
  (let [res @(http/get "https://ip-ranges.amazonaws.com/ip-ranges.json"
                        {:as :stream})
        _ (when-let [err (:error res)]
            (throw+ err))
        ips (with-open [r (io/reader (:body res))]
              (json/parse-stream r true))
        ddbs (->> (:prefixes ips)
                  (filter (fn [p]
                            (and (= core/region (:region p))
                                 (= "DYNAMODB" (:service p)))))
                  (mapv :ip_prefix))]
    ddbs))

(def dynamo-ip-ranges
  "Memoized dynamo-ip-ranges"
  (memoize dynamo-ip-ranges*))

(defn drop!
  "Drops requests from the given CIDR IP range."
  [ip]
  (c/su (c/exec :iptables :-A :INPUT :-s ip :-j :DROP :-w)))

(defrecord PartitionStorageNemesis [ip-ranges]
  n/Nemesis
  (setup! [this test] this)

  (invoke! [this test op]
    (assoc op :value
           (case (:f op)
             :partition-storage
             (c/on-nodes test (:value op)
                         (fn [test node]
                           (mapv drop! ip-ranges)
                           :partitioned))

             :heal-storage
             (net/heal! (:net test) test))))

  (teardown! [this test]
    (net/heal! (:net test) test))

  n/Reflection
  (fs [this]
    #{:heal-storage :partition-storage}))

(defn partition-dynamo-nemesis
  "A nemesis which partitions nodes away from DynamoDB."
  []
  (PartitionStorageNemesis. (dynamo-ip-ranges)))

(defn partition-storage-gen
  "Generator for operations partitioning and healing connections to storage."
  [opts]
  (->> (gen/mix [(repeat (fn [test context]
                           {:type :info
                            :f    :partition-storage
                            :value [(rand-nth (:nodes test))]}))
                 (repeat {:type :info,
                          :f    :heal-storage})])
       (gen/stagger (:interval opts))))

(defn partition-storage-package
  "Nemesis package for isolating nodes from storage."
  [opts]
  (when (contains? (:faults opts) :partition-storage)
    {:nemesis   (partition-dynamo-nemesis)
     :generator (partition-storage-gen opts)
     :final-generator {:type :info, :f :heal-storage}
     :perf #{{:name  "part-store"
              :fs    #{}
              :start #{:partition-storage}
              :stop  #{:heal-storage}
              :color "#E9A447"}}}))

(defn stats-nemesis
  "A nemesis that fetches stats from a random peer."
  ([]
   (stats-nemesis nil))
  ([peers]
   (reify n/Nemesis
     (setup! [this test]
       (stats-nemesis (db/peers test)))

     (invoke! [this test op]
       (assert (= :stats (:f op)))
       (assoc op :value (client/req! (rand-nth peers) :stats nil)))

     (teardown! [this test])

     n/Reflection
     (fs [this]
       #{:stats}))))

(defn stats-gen
  "Generator for stats operations every 30 seconds"
  []
  (->> (repeat {:type :info, :f :stats})
       (gen/delay 30)))

(defn stats-package
  "Nemesis stats package"
  [opts]
  (when (contains? (:faults opts) :stats)
    {:nemesis   (stats-nemesis)
     :generator (stats-gen)
     :perf #{{:name :stats
              :fs #{:stats}
              :hidden? true}}}))

(defn package-gen
  "For long-running tests, it's nice to be able to have periods of no faults,
  periods with lots of faults, just one kind of fault, etc. This takes a time
  period in seconds, which is how long to emit nemesis operations for a
  particular subset of packages. Takes a collection of packages. Constructs a
  nemesis generator which emits faults for a shifting collection of packages
  over time."
  [period packages]
  ; We want a sequence of random subsets of packages
  (repeatedly
    (fn rand-pkgs []
      ; Pick a random selection of packages
      (let [pkgs (vec (take (rand-int (inc (count packages)))
                            (shuffle packages)))
            ; Construct combined generators
            gen       (if (seq pkgs)
                        (apply gen/any (keep :generator pkgs))
                        (gen/sleep period))
            final-gen (keep :final-generator pkgs)]
        ; Ops from the combined generator, followed by a final gen
        [(gen/log (str "Shifting to new mix of nemeses: "
                       (pr-str (map :nemesis pkgs))))
         (gen/time-limit period gen)
         final-gen]))))

(defn nemesis-package
  "Takes CLI opts. Constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)
        packages (->> (nc/nemesis-packages opts)
                      (concat [(partition-storage-package opts)
                               (stats-package opts)])
                      (filter :generator))
        nsp (:stable-period opts)]
    (cond-> (nc/compose-packages packages)
      nsp (assoc :generator (package-gen nsp packages)))))
