(ns jepsen.datomic.workload.grant
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
                            [db :as db]])
  (:import (jepsen.history Op)))

(defn create!
  "Creates a grant and returns its ID."
  [node]
  (let [r (c/req! node :grant {:txn [['jepsen.datomic.peer.grant/create]]})
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
  ; under :value.
  (invoke! [this test {:keys [f value] :as op}]
    (c/with-errors op
      (let [id (create! node)
            txn (case f
                  :approve [['jepsen.datomic.peer.grant/approve id]]
                  :deny    [['jepsen.datomic.peer.grant/deny id]]
                  :approve-deny [['jepsen.datomic.peer.grant/approve id]
                                 ['jepsen.datomic.peer.grant/deny id]])
            r (c/req! node :grant {:txn txn})]
        (assoc op :type :ok, :value (:state' r)))))

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
  :approve-deny ops should always fail. Every grant should be either approved
  xor denied."
  []
  (reify checker/Checker
    (check [this test history check-opts]
      (let [h (->> (h/client-ops history)
                   (h/remove h/invoke?))
            approves         (h/filter-f :approve h)
            denies           (h/filter-f :deny h)
            approve-denies   (h/filter-f :approve-deny h)
            approve-deny-oks (h/oks approve-denies)
            malformed        (keep (fn [op]
                                     (when-let [m (some malformed (:value op))]
                                       {:malformed m
                                        :op op}))
                                   (h/oks h))]
        {:valid?             (and (= 0 (count approve-deny-oks))
                                  (= 0 (count malformed)))
         :approve-deny-oks   (into [] approve-deny-oks)
         :malformed          (into [] malformed)}))))

(defn gen
  "Generator of ops."
  []
  [{:f :approve}
   {:f :deny}
   {:f :approve-deny}])

(defn workload
  "A grant workload, given CLI options."
  [opts]
  {:client    (Client. nil)
   :generator (gen)
   :checker   (checker)})
