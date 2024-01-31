(ns jepsen.datomic.workload.internal
  "Looks at consistency *inside* a single transaction. The checker for this
  workload involves two sub-checkers. One, :datomic, validates Datomic's
  current behavior, and is essentially a regression test. The second,
  :standard, is based on how transactions would work in a standard serializable
  database."
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

(defn cas-cas!
  "Takes a node, a value, and a pair of [v v'] cas values. Creates an entity
  with value, then tries to cas it from v1->v1', then v2->v2'in a txn. Returns
  [r r2] for the two transaction results."
  [node v [v1 v1'] [v2 v2']]
  (let [r (c/req! node :internal {:txn [[:db/add "x" :internal/key "x"]
                                        [:db/add "x" :internal/value v]]})
        state' (:state' r)
        _  (assert (= 1 (count state')))
        id (:id (first state'))
        r2 (c/req! node :internal
                   {:txn [[:db/cas id :internal/value v1 v1']
                          [:db/cas id :internal/value v2 v2']]})]
    [r r2]))

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
  (invoke! [this test op]
    (c/with-errors op
      (case (:f op)
        ; Special case: cas 0->1, 1->2
        :cas-cas
        (assoc op :type :ok, :value (cas-cas! node 0 [0 1] [1 2]))

        ; Special case again, this time cas 0->1, 0->2
        :cas-cas2
        (assoc op :type :ok, :value (cas-cas! node 0 [0 1] [0 2]))

        ; Special case: cas 0->1, 0->1
        :cas-cas3
        (assoc op :type :ok, :value (cas-cas! node 0 [0 1] [0 1]))

        ; Normally, just execute transactions in sequence
        (let [results (mapv (fn [txn]
                              (c/req! node :internal {:txn txn}))
                            (:value op))]
          (assoc op :type :ok, :value results)))))

  (teardown! [this test])

  (close! [this test]))

(def reset
  "A reset operation."
  {:f     :reset
   :value [[['jepsen.datomic.peer.internal/reset]]]})

(defn check-ok
  "Checks that an operation should have been :type :ok"
  [{:keys [type] :as op}]
  (when (not= type :ok)
    {:type :expected-ok}))

(defn check-fail
  "Checks that an operation should have been :type :fail"
  [{:keys [type] :as op}]
  (when (not= type :fail)
    {:type :expected-fail}))

(defn check-conflict
  "Checks an operation for a conflict, making sure it fails and returns a
  datoms-conflict error."
  [{:keys [f type value error] :as op}]
  (or (check-fail op)
      (when (-> error second :db/error (not= :db.error/datoms-conflict))
        {:type :expected-datoms-conflict})))

(defn check-cas-failed
  "Checks that an operation returned a 'db.error/cas-failed' error."
  [{:keys [error] :as op}]
  (or (check-fail op)
      (when (-> error first (not= :cas-failed))
        {:type :expected-cas-failed
         :error error
         :op    op})))

(defn check-state'
  "Checks that the state after a given txn is compatible with the given state."
  [op expected res]
  (or (check-ok op)
      (when-not (= expected
                   (->> (:state' res)
                        (map (juxt :k :v))
                        (into {})))
        {:type :wrong-state'
         :expected expected
         :actual   (:state' res)})))

(def cases
  "A series of unique ops to perform, each with a unique :f and a :value of a
  series of transactions to perform."
  [
   ; Adding something and then changing its value in a single transaction
   ; should be the same as if you change the value in a separate transaction
   {:f   :add-change
    :value [[[:db/add "x" :internal/key "x"]
             [:db/add "x" :internal/value 0]
             [:db/add "x" :internal/value 1]]]
    :standard (fn [op]
                (check-state' op {"x" 1} (first (:value op))))
    ; But in Datomic, it's illegal
    :datomic check-conflict}

   ; Adding and retracting something should result in the fact not being
   ; present; that's what would happen if you did two transactions
   {:f   :add-retract
    :value [[[:db/add      "x" :internal/key "x"]
             [:db/retract  "x" :internal/key "x"]]]
    :standard (fn [op]
                (check-state' op {} (first (:value op))))
    ; In Datomic, it's illegal
    :datomic check-conflict}

   ; CaS on a single row twice, going 0->1, 1->2
   {:f     :cas-cas
    :value nil ; Specially handled by client
    :standard (fn [op]
                (let [[r1 r2] (:value op)]
                  (or (check-state' op {"x" 0} r1)
                      ; We go from 0 -> 1, then 1 -> 2.
                      (check-state' op {"x" 2} r2))))
    ; Datomic's second cas fails to see the first, and the txn explodes
    :datomic check-cas-failed}

   ; CaS on a single row twice, going 0->1, 0->2
   {:f     :cas-cas2
    :value nil ; Specially handled by client
    ; This *should* yield a CaS error, because the second CaS is illegal!
    :standard check-cas-failed
    ; Instead, Datomic lets both CaS calls happen, and then flags contradictory
    ; outputs
    :datomic check-conflict}

   ; Cas on a singel row twice, going 0->1, 0->1.
   {:f :cas-cas3
    :value nil ; Specially handled by client
    ; This should yield a CaS error, because the second CaS is illega!
    :standard check-cas-failed
    ; Instead, Datomic lets both CaS calls happen
    :datomic (fn [op]
               (let [[r1 r2] (:value op)]
                 (or (check-state' op {"x" 0} r1)
                     ; Datomic doesn't care: both CaS ops run, producing 1.
                     (check-state' op {"x" 1} r2))))}


   ; Double-incrementing a row should not lose an update
   {:f   :inc
    :value [; Create x = 0
            [[:db/add "x" :internal/key "x"]
             [:db/add "x" :internal/value 0]]
            ; First one increment
            [['jepsen.datomic.peer.internal/increment "x"]]
            ; Then two in one txn
            [['jepsen.datomic.peer.internal/increment "x"]
             ['jepsen.datomic.peer.internal/increment "x"]]]
    :standard (fn [op]
                (let [[r1 r2 r3] (:value op)]
                  ; State after the initial write should be x = 0
                  (or (check-state' op {"x" 0} r1)
                      ; The single increment should produce x = 1
                      (check-state' op {"x" 1} r2)
                      ; The double-increment should produce x = 3
                      (check-state' op {"x" 3} r3))))
    ; In Datomic, the double-increment is a noop.
    :datomic (fn [op]
               (let [[r1 r2 r3] (:value op)]
                 ; State after the initial write should be x = 0
                 (or (check-state' op {"x" 0} r1)
                     ; The single increment should produce x = 1
                     (check-state' op {"x" 1} r2)
                     ; The double-increment actually drops the second fn
                     (check-state' op {"x" 2} r3))))}

   ; Retracting then adding something should result in the fact being true:
   ; that's what would happen if you did this in two txns
   {:f     :retract-add
    :value [[[:db/retract "x" :internal/key "x"]
             [:db/add     "x" :internal/key "x"]
             [:db/add     "x" :internal/value 0]]]
    :standard (fn [op]
                (check-state' op {"x" 0} (first (:value op))))
    ; In Datomic it's illegal
    :datomic check-conflict}
   ])

(defn check-case
  "Takes a check mode (e.g. :datomic or :standard). Checks a specific
  completion op, returning nil if nothing bad happened, or an error map if it
  went wrong. Each case has a different way of being checked."
  [mode {:keys [f type value error] :as op}]
  (let [spec (-> (group-by :f cases)
                 (get f)
                 first
                 (get mode))]
    (assert spec (str "No specification for f " f " and mode " mode))
    (spec op)))

(defn checker
  "Checker for internal histories. Each case has a different checker which
  validates that specific transaction's properties."
  [mode]
  (reify checker/Checker
    (check [this test history check-opts]
      (let [errs (->> (h/client-ops history)
                      (h/remove h/invoke?)
                      (h/remove (comp #{:reset} :f))
                      (keep (fn [op]
                              (when-let [err (check-case mode op)]
                                (assoc err :f (:f op) :op op)))))]
        {:valid? (not (seq errs))
         :count  (count errs)
         :errs   (take 128 errs)}))))

(defn gen
  "Generator of ops. This generator relies on singlethreaded behavior: it may
  do weird things with faults. We can't do an entire case inside a transaction
  because we're not sure transactions actually *work* correctly."
  []
  ; We want a singlethreaded test
  (->> cases
       (map #(select-keys % [:f :value]))
       ;cycle
       (interpose reset)
       (gen/on-threads #{0})))

(defn workload
  "An internal workload, given CLI options."
  [opts]
  {:client    (Client. nil)
   :generator (gen)
   :checker   (checker/compose
                {:datomic (checker :datomic)
                 :standard (checker :standard)})})
