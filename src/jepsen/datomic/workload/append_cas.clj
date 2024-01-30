(ns jepsen.datomic.workload.append-cas
  "List append, based on Datomic CaS. Should be strong session SI by default,
  since we only CaS writes."
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
            [jepsen.datomic.workload [append :as a]]
            [jepsen.tests.cycle.append :as append])
  (:import (jepsen.history Op)))

(defn checker
  "Checker for Datomic histories. Running with :sync should yield a strong SI
  system. Otherwise, we expect strong session SI."
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

        ; Running with sync should give a strong SI system
        (:sync test)
        (elle/check (assoc (opts "elle")
                           :consistency-models [:strong-snapshot-isolation])
                    history)

        ; Otherwise, we check for strong session
        :else
        (elle/check (assoc (opts "elle")
                           :consistency-models
                           [:strong-session-snapshot-isolation])
                    history))))))

(defn workload
  "A list-append workload."
  [opts]
  ; We're basically the same as the append test, just a different path for our
  ; client, and using a different checker.
  (-> (a/workload opts)
      (assoc :client (a/->Client :txn-cas nil)
             :checker (checker))))
