(ns jepsen.datomic.peer.append
  "Implementation of a basic append workload."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [datomic.api :as d]
            [jepsen.datomic.peer [core :as c]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def schema
  [{:db/ident       :append/key
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "The unique identifier for an append workload list."}
   {:db/ident       :append/elements
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/many
    :db/doc         "A single element in an append workload"}])

(def read-q
  "A query which finds all elements associated with a given key. We derive the
  order from the associated txInstant, and then (relying on the fact that
  elements are monotone inside a transaction) the value orders."
  '{; Having a :keys clause/key present in the query causes the :find values to
    ; be returned as a map (oooooh but it's also a tuple still!), kinda like
    ; `(zipmap keys find)`.
    :find [?time ?element]
    :keys [:time :element]
    :in   [$ ?k]
    :where [[?list :append/key ?k]
            [?list :append/elements ?element ?tx]
            [?tx   :db/txInstant ?time]]})

(defn apply-txn
  "Datomic has no concept of an interactive transaction, or a stored procedure
  which can return data to a caller. Writes and reads are strongly separated:
  you can perform arbitrary queries over an observed state of the DB, or
  execute a stored procedure which performs arbitrary read queries and finally
  produces an arbitrary set of writes.

  All of our Elle analysis tooling so far has been built for systems which
  offer *either* interactive transactions or stored procedures with a read
  channel. To implement this in Datomic, we use this function, which takes a
  database state and a Jepsen transaction, interprets the txn, and returns a
  [datomic-txn, jepsen-txn] pair. We use the Datomic transaction for the
  writes, and then go back and run this function again, locally, on the
  pre-state of the database once in order to figure out what the reads would
  have been, and return that completed jepsen-txn to the client."
  [db txn]
  ; First, collect read/writes
  (let [grouped    (group-by first txn)
        reads      (get grouped :r)
        appends    (get grouped :append)
        ; Construct write set
        datomic-txn (mapv (fn [[f k v]]
                            {:append/key k
                             :append/elements v})
                          appends)
        ; Query db for state of all read keys. We derive the order from the
        ; associated txInstant, and then (relying on the fact that elements are
        ; monotone inside a transaction) the value orders.
        state (reduce
                (fn read-k [state k]
                  (let [r (d/q read-q db k)]
                    (prn :r r)
                    (->> r
                         (sort-by (juxt :time :element))
                         (mapv :element)
                         (assoc state k))))
                {}
                (set (mapv second reads)))
        ;_ (prn :state)
        ;_ (pprint state)
        ; Quick interpreter: run through micro-ops and apply each to state
        [_s datomic-txn jepsen-txn']
        (reduce
          (fn mop [[state datomic-txn jepsen-txn'] [f k v :as mop]]
            (case f
              :r
              [state
               datomic-txn
               (conj jepsen-txn' [f k (get state k)])]

              :append
              [(assoc state k (conj (get state k []) v))
               (conj datomic-txn {:append/key k, :append/elements v})
               (conj jepsen-txn' mop)]))
          [state [] []]
          txn)]
    [datomic-txn jepsen-txn']))

(defn apply-txn-datomic
  "Like apply-txn, but transforms txn back into Clojure data, and just returns
  the Datomic txn data."
  [db txn]
  (->> txn
       c/->clj
       (apply-txn db)
       first))

(defn handle-txn
  "Handles a txn request"
  [conn txn]
  ;(info :handle-txn txn)
  ; First, submit the transaction for writing
  (let [{:keys [db-before]}
        @(d/transact conn [['jepsen.datomic.peer.append/apply-txn-datomic txn]])
        ; Re-run the query to get the completed txn.
        [_ txn'] (apply-txn db-before txn)]
    txn'))
