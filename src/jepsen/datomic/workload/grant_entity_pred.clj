(ns jepsen.datomic.workload.grant-entity-pred
  "Simulates a grant application process where grants should either be approved
  or denied, but never both. Tries to do both."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [history :as h]
                    [store :as store]]
            [jepsen.datomic [client :as c]
                            [db :as db]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (jepsen.history Op)))

(defn create!
  "Creates a grant and returns its ID."
  [node]
  (let [r (c/req! node :grant {:txn [['jepsen.datomic.peer.grant-entity-pred/create]]})
        ; Probably a hack, but we know that a create txn produces exactly one
        ; datom, and the first datom is the txn record itself, so it's probably
        ; here:
        id (get-in r [:datoms 1 0])]
    id))

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

  ; Runs a series of transactions in sequence, saving the results in a vector
  ; of state maps under the resulting op's :value
  (invoke! [this test {:keys [f value] :as op}]
    (try+
      (c/with-errors op
        (let [id   (create! node)
              txns (case f
                     :approve [[['jepsen.datomic.peer.grant-entity-pred/approve id]]]
                     :deny    [[['jepsen.datomic.peer.grant-entity-pred/deny id]]]
                     ; Runs an approve txn, then a deny txn
                     :approve-then-deny
                     [[['jepsen.datomic.peer.grant-entity-pred/approve id]]
                      [['jepsen.datomic.peer.grant-entity-pred/deny id]]]
                     ; Runs approve and deny in the same txn
                     :approve-deny [[['jepsen.datomic.peer.grant-entity-pred/approve id]
                                     ['jepsen.datomic.peer.grant-entity-pred/deny id]]])
              r (mapv (fn [txn] (:state' (c/req! node :grant {:txn txn})))
                      txns)]
          (assoc op :type :ok, :value r)))
      (catch [:type :already-approved-or-denied] _
             (assoc op :type :ok, :error :already-approved-or-denied))
      (catch [:type :already-approved] _
        (assoc op :type :fail, :error :already-approved))
      (catch [:type :already-denied] _
        (assoc op :type :fail, :error :already-denied))))

  (teardown! [this test])

  (close! [this test]))

(defn malformed
  "Examines a single grant to see if it's both approved and denied. Returns
  grant if malformed, nil otherwise."
  [grant]
  (when (and (:grant/approved-at grant)
             (:grant/denied-at grant))
    grant))

(defn checker
  "Checker for grant histories. :approve and :deny ops should be fine.
  :approve-then-deny ops, :approve-deny ops should always fail. Every grant
  should be either approved xor denied."
  []
  (reify checker/Checker
    (check [this test history check-opts]
      (let [h (->> (h/client-ops history)
                   (h/remove h/invoke?))
            approves         (h/filter-f :approve h)
            denies           (h/filter-f :deny h)
            approve-denies   (h/filter-f :approve-deny h)
            approve-deny-oks (h/oks approve-denies)
            approve-then-deny-oks (->> (h/filter-f :approve-then-deny h)
                                      (h/oks))
            malformed        (keep (fn [op]
                                     (let [state' (last (:value op))]
                                       (when-let [m (some malformed state')]
                                         {:malformed m
                                          :op op})))
                                   (h/oks h))]
        {:valid?             (and (= 0 (count approve-deny-oks))
                                  (= 0 (count approve-then-deny-oks))
                                  (= 0 (count malformed)))
         :approve-then-deny-oks (into [] approve-then-deny-oks)
         :approve-deny-oks      (into [] approve-deny-oks)
         :malformed             (into [] malformed)}))))

(defn gen
  "Generator of ops."
  []
  [{:f :approve}
   {:f :deny}
   {:f :approve-then-deny}
   {:f :approve-deny}])

(defn workload
  "A grant workload, given CLI options."
  [opts]
  {:client    (Client. nil)
   :generator (gen)
   :checker   (checker)})
