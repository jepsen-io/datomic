(ns jepsen.datomic.workload.append
  "A simple append workload."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [elle.list-append :as elle]
            [jepsen [client :as client]
                    [checker :as checker]
                    [history :as h]
                    [store :as store]]
            [jepsen.datomic [client :as c]
                            [db :as db]]
            [jepsen.tests.cycle.append :as append])
  (:import (jepsen.history Op)))

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
      (let [{:keys [txn t t' state]}
            (c/req! node :txn {:txn  (:value op)
                               :sync? (:sync test)})]
        (assoc op
               :type  :ok
               :value txn
               :t     t
               :t'    t'
               :state state))))

  (teardown! [this test])

  (close! [this test]))

(defn write?
  "Does an operation perform a txn with at least one write?"
  [^Op op]
  (when (identical? :txn (.f op))
    (loopr []
           [[f] (.value op)]
           (if (identical? f :append)
             true
             (recur))
           false)))

(defn checker
  "Checker for Datomic histories. Running with :sync should yield a strict
  serializable system. Otherwise, we verify the entire history is
  strong-session-serializable, and the history restricted to just writes is
  strong serializable."
  []
  (reify checker/Checker
    (check [this test history check-opts]
      (let [; Options for elle.list-append/check
            opts (fn [dir-name]
                   {:directory
                    (.getCanonicalPath
                      (store/path! test (:subdirectory check-opts) dir-name))})]
      (cond ; Users can specify their own consistency model
            (:consistency-model test)
            (do (info "Using custom consistency model"
                      (:consistency-model test))
                (elle/check (assoc (opts "elle")
                                   :consistency-models
                                   [(:consistency-model test)])
                            history))

        ; Running with sync should give a strict-1SR system
        (:sync test)
        (elle/check (assoc (opts "elle")
                           :consistency-models [:strong-serializable])
                    history)

        ; Otherwise, we check that writes are strong
        :else
        (let [writes
              (elle/check
                (assoc (opts "elle-writes")
                       :consistency-models [:strong-serializable])
                (h/filter write? history))
              ; And that writes+reads are strong session
              all
              (elle/check
                (assoc (opts "elle")
                       :consistency-models [:strong-session-serializable])
                history)]
          {:valid?  (checker/merge-valid (map :valid? [writes all]))
           :writes  writes
           :all     all}))))))

(defn workload
  "A list-append workload"
  [opts]
  (-> (select-keys opts [:key-count
                         :min-key-length
                         :max-txn-length
                         :max-writes-per-key])
      append/test
      (assoc :client (Client. nil)
             :checker (checker))))
