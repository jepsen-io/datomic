(ns jepsen.datomic.peer.append-cas
  "An append workload which stores values in a single attribute, updating that
  value with CaS."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [datomic.api :as d]
            [jepsen.datomic.peer [core :as c]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (datomic.impl Exceptions$IllegalStateExceptionInfo)))

(def schema
  [{:db/ident       :append-cas/key
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "The unique identifier for an append-cas list."}
   {:db/ident       :append-cas/elements
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "A comma-separated string of elements for an append-cas list"}])

(defn k->id
  "Takes a db state and a key, returns the entity ID for that key."
  [db k]
  (-> '{:find [?list]
       :keys [:id]
       :in   [$ ?k]
       :where [[?list :append-cas/key ?k]]}
      (d/q db k)
      first
      :id))

(def read-q
  "A query which finds the entity ID and string list of elements associated
  with a given key."
  '{; Having a :keys clause/key present in the query causes the :find values to
    ; be returned as a map (oooooh but it's also a tuple still!), kinda like
    ; `(zipmap keys find)`.
    :find [?id ?elements]
    :keys [:id :elements]
    :in   [$ ?k]
    :where [[?id :append-cas/key ?k]
            [?id :append-cas/elements ?elements]]})

(defn read-k
  "Reads the current entity ID and value of list k from the DB, as a string.
  Returns a map of :id, :elements. Returns nil when not present."
  [db k]
  (first (d/q read-q db k)))

(defn decode
  "Decodes a comma separate string to a vector of longs. Nil decodes to []."
  [xs]
  (if (nil? xs)
    []
    (->> (str/split xs #",")
         (mapv parse-long))))

(defn encode
  "Encodes a vector of longs as a string."
  [xs]
  (str/join "," xs))

(defn apply-txn
  "Runs a txn on a database state, producing [datomic-txn txn']. We use db/with
  to play forward the state of the DB for internal reads. Our transaction ops
  are all db/cas calls for updates, and a literal key+elements pair for the
  insert."
  [db txn]
  (loop [txn         txn
         db'         db
         datomic-txn []
         txn'        []]
    (if-not (seq txn)
      ; Done
      [datomic-txn txn']
      ; Handle mop
      (let [[f k v :as mop] (first txn)]
        (info :mop mop :datomic-txn datomic-txn :txn' txn')
        (case f
          :r
          (let [v (-> db' (read-k k) :elements decode)]
            (recur (next txn) db' datomic-txn (conj txn' [f k v])))

          :append
          (if-let [{:keys [id elements]} (read-k db' k)]
            ; We have an existing entity and can emit a CaS for it.
            (let [elements' (-> (decode elements)
                              (conj v)
                              encode)
                  ops [[:db/cas id :append-cas/elements elements elements']]
                        [; Ensure an entity with this key exists. We do this every
                         ; single time--even if it already exists--because it
                         ; forces the temp ID (str k) to resolve to the real entity
                         ; ID, and we need that ID for the CAS op.
                         [:db/add (str k) :append-cas/key k]
                         ; Update via CaS. Note that elements will be nil if it
                     ; doesn't exist.
                     [:db/cas (str k) :append-cas/elements elements elements']]]
            (info :ops (pr-str ops))
            (recur (next txn)
                   ; Simulate application on local DB state
                   (:db-after (d/with db' ops))
                   (into datomic-txn ops)
                   (conj txn' mop))))))))

(defn db
  "Gets the state of the DB, optionally syncing."
  [conn sync?]
  (if sync?
    ; Do a synchronous read
    (let [;_ (info "Performing synchronous read")
          db (deref (d/sync conn) 1000 ::timeout)]
      (when (= db ::timeout)
        (throw+ {:type :sync-timeout, :definite? true}))
      db)
    ; Use local cache
    (do ;(info "Performing async read")
        (d/db conn))))

(defn handle-txn
  "Handles a txn request"
  [conn {:keys [txn sync?] :as req}]
  (info :handle-txn txn)
  (let [db (try+
             (db conn sync?)
             ; Any failures here are definite, since we never affect
             ; state
             (catch map? e
               (throw+ (assoc e :definite? true))))
        ; Simulate txn on this state
        [datomic-txn txn'] (apply-txn db txn)
        _ (info :datomic-txn (pr-str datomic-txn))
        ; Apply resulting datomic txn
        {:keys [db-before db-after]}
        (if (seq datomic-txn)
          (try+ @(d/transact conn datomic-txn)
                (catch [:cognitect.anomalies/category
                        :cognitect.anomalies/conflict] e
                  ; A DB conflict is a definite error.
                  (info :conflict (pr-str txn') (pr-str datomic-txn)
                        "\n"
                        (with-out-str (pprint e)))
                  (throw+ (assoc e :definite? true)))
                (catch [:cognitect.anomalies/category
                        :cognitect.anomalies/unavailable] e
                  ; This is *not* a definite error.
                  (throw+ (assoc e :definite? false))))
          ; Read-only, no effects
          {:db-before db, :db-after db})]
    {:read-t  (d/basis-t db)
     :write-t (d/basis-t db-after)
     :txn      txn'}))
