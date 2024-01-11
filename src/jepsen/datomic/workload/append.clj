(ns jepsen.datomic.workload.append
  "A simple append workload."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]]
            [jepsen.datomic [client :as c]
                            [db :as db]]
            [jepsen.tests.cycle.append :as append]))

(defrecord Client [node]
  client/Client
  (open! [this test node]
    ; Some of our nodes don't answer queries, so we re-map everyone to other
    ; nodes.
    (let [nodes (:nodes test)
          i     (.indexOf nodes node)
          peers (->> (:nodes test)
                     (filter (partial db/peer? test))
                     (into []))
          node  (nth peers (mod i (count peers)))]
    (assoc this :node node)))

  (setup! [this test])

  (invoke! [this test op]
    (c/with-errors op
      (assoc op
             :type :ok
             :value (c/req! node [:txn] (:value op)))))

  (teardown! [this test])

  (close! [this test]))

(defn workload
  "A list-append workload"
  [opts]
  (-> (select-keys opts [:key-count
                         :min-key-length
                         :max-txn-length
                         :max-writes-per-key])
      (assoc :consistency-models [:strong-session-serializable])
      append/test
      (assoc :client (Client. nil))))
