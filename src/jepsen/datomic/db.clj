(ns jepsen.datomic.db
  "Automates the installation and teardown of Datomic clusters."
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
                            [client :as client]]
            [jepsen.datomic.db [datomic :as datomic]
                               [peer :as peer]
                               [storage :as storage]
                               [transactor :as transactor]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn node-roles
  "Splits up the nodes in a test into :transactor and :peer roles"
  [test]
  (zipmap (:nodes test)
          (concat [:transactor :transactor] (repeat :peer))))

(defn transactor?
  "Is this node a transactor?"
  [test node]
  (= :transactor (get (node-roles test) node)))

(defn peer?
  "Is this node a peer?"
  [test node]
  (= :peer (get (node-roles test) node)))

(defn peers
  "All peers in a test."
  [test]
  (->> (node-roles test)
       (keep (fn [[node role]]
               (when (= role :peer)
                 node)))

       vec))

(defn install-prereqs!
  "Prerequisites for DB, transactors, etc."
  []
  (debian/install ["openjdk-17-jdk-headless"]))

(defrecord DB [datomic transactor peer]
  db/DB
  (setup! [this test node]
    (install-prereqs!)
    (when (peer? test node)
      (db/setup! peer test node))
    (when (transactor? test node)
      (db/setup! datomic test node)
      (db/setup! transactor test node)))

  (teardown! [this test node]
    (when (transactor? test node)
      (db/teardown! transactor test node)
      (db/teardown! datomic test node))
    (when (peer? test node)
      (db/teardown! peer test node)))

  db/LogFiles
  (log-files [this test node]
    (merge (db/log-files transactor test node)
           (db/log-files peer test node)))

  db/Kill
  (start! [this test node]
    {:transactor (when (transactor? test node)
                   (db/start! transactor test node))
     :peer (when (peer? test node)
             (db/start! peer test node))})

  (kill! [this test node]
    {:transactor (when (transactor? test node)
                   (db/kill! transactor test node))
     :peer (when (peer? test node)
             (db/kill! peer test node))})

  db/Pause
  (pause! [this test node]
    {:transactor (when (transactor? test node)
                   (db/pause! transactor test node))
     :peer (when (peer? test node)
             (db/pause! peer test node))})

  (resume! [this test node]
    {:transactor (when (transactor? test node)
                   (db/resume! transactor test node))
     :peer (when (peer? test node)
             (db/resume! peer test node))}))

(defn db
  "Takes CLI options. Constructs a fresh Jepsen DB with Datomic, storage,
  transactor, and peer components"
  [opts]
  (let [datomic     (datomic/db opts)
        storage     (storage/db opts)
        peer        (peer/db opts storage)
        transactor  (transactor/db opts storage peer)]
    (map->DB {:datomic    datomic
              :transactor transactor
              :peer       peer})))
