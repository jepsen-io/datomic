(ns jepsen.datomic.peer.grant-entity-pred
  "Simulates a grant application process, where grants are created, then either
  approved or denied, but never both."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [datomic.api :as d]
            [jepsen.datomic.peer [core :as c]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util Date)))

(defn valid-grant?
  "Returns true iff the provided eid for a grant has only
  approved-at or denied-at, never both."
  [db eid]
  (let [{:grant/keys [approved-at denied-at]} (d/pull db '[:grant/approved-at :grant/denied-at] eid)]
    (not (and approved-at denied-at))))

(def schema
  [{:db/ident       :grant/created-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc         "When was this grant created?"}
   {:db/ident       :grant/denied-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc         "When was this grant denied?"}
   {:db/ident       :grant/approved-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc         "When was this grant approved?"}
   {:db/ident        :grant/valid?
    :db.entity/preds ['jepsen.datomic.peer.grant-entity-pred/valid-grant?]
    :db/doc          "Ensures transaction this is invoked in doesn't violate domain-specific invariants"}])

(defn all-ids
  "All grant entity IDs."
  [db]
  (->> (d/q '{:find  [?id]
              :where [[?id :grant/created-at _]]}
            db)
       (map first)))

(defn read-*
  "Reads all grants as maps."
  [db]
  (->> (all-ids db)
       (map (partial d/entity db))
       ; Hard-won knowledge: the maps returned by d/entity print as if they
       ; were maps with only a :db/id key, and they *will* respond to (:db/id
       ; entity) with that ID. But if you ask for their keys, they'll tell you
       ; they have no knowledge of :db/id, and instead they have other keys.
       ; Doing (into {} entity) produces maps with no :db/id either. We produce
       ; plain old maps with both IDs and values.
       ;
       ; API: does something psychotic
       ; Her gays: honestly, work
       (map (fn [entity]
              (assoc (into {} entity)
                     :db/id (:db/id entity))))))

(defn reset
  "A transaction fn to clear everything in the DB."
  [db]
  (mapv (fn [entity]
          [:db/retractEntity entity])
        (all-ids db)))

(defn create
  "Creates a new grant."
  [db]
  [[:db/add "g" :grant/created-at (Date.)]])

(defn approved?
  "Has a grant been approved?"
  [db id]
  (pos? (count (d/q '{:find  [?t]
                      :in    [$ ?id]
                      :where [[?id :grant/approved-at ?t]]}
                    db
                    id))))

(defn denied?
  "Has a grant been denied?"
  [db id]
  (pos? (count (d/q '{:find  [?t]
                      :in    [$ ?id]
                      :where [[?id :grant/denied-at ?t]]}
                    db
                    id))))

(defn assert-fresh
  "Throws if the given grant ID is approved or denied."
  [db id]
  (when (approved? db id)
    (throw+ {:definite? true, :type :already-approved}))
  (when (denied? db id)
    (throw+ {:definite? true, :type :already-denied})))

(defn approve
  "Approves a grant by ID. Ensures the grant has not been approved or denied."
  [db id]
  (assert-fresh db id)
  [{:db/id             id
    :grant/approved-at (Date.)
    :db/ensure         :grant/valid?}])

(defn deny
  "Denies a grant by ID. Ensures the grant has not been approved or denied."
  [db id]
  (assert-fresh db id)
  [{:db/id           id
    :grant/denied-at (Date.)
    :db/ensure       :grant/valid?}])

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
          (catch [:db/error :db.error/entity-pred] e
            ; This is *not* a definite error since it's the entity pred being enforced
            (throw+ (assoc e :definite? false :type :already-approved-or-denied)))
          (catch [:cognitect.anomalies/category
                  :cognitect.anomalies/unavailable] e
            ; This is *not* a definite error.
            (throw+ (assoc e :definite? false))))]
    ;(info (with-out-str (pprint r)))
    {:state' (read-* db-after)
     :datoms (mapv (juxt :e :a :v :tx :added) (:tx-data r))}))
