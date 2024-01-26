(ns jepsen.datomic.db.storage
  "Automates the installation and teardown of Datomic storage"
  (:require [clojure [edn :as edn]
                     [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java [io :as io]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [core :as jepsen]
                    [util :refer [with-thread-name]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.datomic [aws :as aws]
                            [core :as core]
                            [client :as client]]
            [jepsen.datomic.db [datomic :as datomic]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

(def table
  "The DynamoDB table we use when we're the ones controlling it."
  "datomic-jepsen")

(defprotocol Storage
  (uri [this test]
       [this test db-name]
       "Returns the URI for this storage system.")

  (properties-snippet [this test]
                      "Constructs a snippet of the transactor.properties file
                      which specifies how to connect to this storage system."))

(defrecord Dynamo []
  db/DB
  (setup! [this test node]
    (when (= node (jepsen/primary test))
      ; See https://docs.datomic.com/pro/overview/storage.html#automated-setup
      (info "Creating DynamoDB tables")
      (datomic/datomic! :ensure-transactor
                        datomic/properties-file
                        datomic/properties-file)
      (aws/provision-io! test table)))

  (teardown! [this test node]
    (when (= node (jepsen/primary test))
      (info "Tearing down DynamoDB")
      (aws/delete-all! table)))

  Storage
  (uri [this test]
    (uri this test core/db-name))

  (uri [this test db-name]
    (str "datomic:ddb://" core/region "/" table "/" db-name))

  (properties-snippet [this test]
    (str "aws-dynamodb-table=" table "\n"
         "aws-dynamodb-region=" core/region)))

(defn dynamo
  "A Jepsen DB which supports creating and destroying DynamoDB tables."
  [opts]
  (Dynamo.))

(defrecord ExtantDynamo []
  db/DB
  (setup! [this test node])

  (teardown! [this test node])

  Storage
  (uri [this test]
    (uri this test core/db-name))

  (uri [this test db-name]
    (str "datomic:ddb://" core/region "/" (:dynamo-table test) "/" db-name))

  (properties-snippet [this test]
    (str "aws-dynamodb-table=" (:dynamo-table test) "\n"
         "aws-dynamodb-region=" core/region "\n"
         "aws-transactor-role=" (:aws-transactor-role test) "\n"
         "aws-peer-role=" (:aws-peer-role test) "\n")))

(defn extant-dynamo
  "A Jepsen DB which uses a pre-existing DynamoDB table, configured in the test
  map."
  [opts]
  (ExtantDynamo.))

(defn db
  "Constructs a storage DB given CLI options."
  [opts]
  (if (:dynamo-table opts)
    (do (assert (:aws-transactor-role opts))
        (assert (:aws-peer-role opts))
        (extant-dynamo opts))
    (dynamo opts)))
