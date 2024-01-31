(ns jepsen.datomic.peer.internal
  "Supports tests for internal consistency."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [datomic.api :as d]
            [jepsen.datomic.peer [core :as c]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def schema
  [{:db/ident       :internal/key
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "The unique identifier for an internal workload."}
   {:db/ident       :internal/value
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "A single value in an internal workload."}])

(def read-q
  "A query which finds the entity ID and value associated with a key."
  '{:find [?id ?value]
    :keys [:id :value]
    :in   [$ ?k]
    :where [[?id :internal/key ?k]
            [?id :internal/value ?value]]})

(defn read-k
  "Reads the current {:id ..., :value ...} of key k from the DB."
  [db k]
  (first (d/q read-q db k)))

(defn increment
  "A transaction fn to increment the value for a given key by one."
  [db k]
  (let [{:keys [id value]} (read-k db k)]
    [[:db/add id :internal/value (inc value)]]))

(defn reset
  "A transaction fn to clear everything in the DB."
  [db]
  (let [entities (->> (concat (d/q '{:find  [?id]
                                     :where [[?id :internal/key]]}
                                   db)
                              (d/q '{:find  [?id]
                                     :where [[?id :internal/value]]}
                                   db))
                      (map first)
                      distinct)]
    (info "Entities: " (pr-str entities))
    (mapv (fn [entity]
            [:db/retractEntity entity])
          entities)))

(defn read-*
  "Reads all internal records as maps."
  [db]
  (-> '{:find [?id ?k ?v]
        :keys [:id :k :v]
        :where [[?id :internal/key ?k]
                [?id :internal/value ?v]]}
      (d/q db)))

(defn handle-txn
  "Handles a txn request. Returns a map of:

  :t      The time just before the txn
  :t'     The time just after the txn
  :state  All records prior to the txn
  :state' All records after the txn
  "
  [conn {:keys [txn] :as req}]
  (info :handle-txn txn)
  (let [; Run transaction directly
        {:keys [db-before db-after] :as r}
        (try+
          @(d/transact conn txn)
          (catch [:cognitect.anomalies/category
                  :cognitect.anomalies/incorrect] e
            ; This represents things like an illegal txn
            (throw+ (assoc e :definite? true)))
          (catch [:cognitect.anomalies/category
                  :cognitect.anomalies/unavailable] e
            ; This is *not* a definite error.
            (throw+ (assoc e :definite? false))))]
    (info (with-out-str (pprint r)))
    {:t      (d/basis-t db-before)
     :t'     (d/basis-t db-after)
     :state  (read-* db-before)
     :state' (read-* db-after)}))
