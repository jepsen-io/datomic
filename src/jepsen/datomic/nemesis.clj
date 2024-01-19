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
            [jepsen.datomic [core :as dc]]
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
                            (and (= dc/region (:region p))
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

(defn nemesis-package
  "Takes CLI opts. Constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)]
    (->> (nc/nemesis-packages opts)
         (concat [(partition-storage-package opts)])
         (remove nil?)
         nc/compose-packages)))
