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
            [jepsen.datomic.db [peer :as db.peer]
                               [transactor :as db.transactor]]
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

(defn install-prereqs!
  "Prerequisites for DB, transactors, etc."
  []
  (debian/install ["openjdk-17-jdk-headless"]))

(defrecord DB [transactor peer]
  db/DB
  (setup! [this test node]
    ; This is a little awkward--there's lots of ordering dependencies between
    ; transactor and peer, but we want to parallelize as much as we can for
    ; speed, so we actually drive a lot of the setup process ourselves instead
    ; of calling db/setup!
    (install-prereqs!)
    ; Installing the peer is slow, so we parallelize installation/config
    (let [peer-fut
          (future
            (with-thread-name (str "jepsen node " node " peer")
              (cond (peer? test node)
                    (db/setup! peer test node)

                    ; We still want a copy of the peer lib on the
                    ; classpath so we can call its fns in transactions.
                    (transactor? test node)
                    (db.peer/install! test))))
          transactor-fut
          (future
            (with-thread-name (str "jepsen node " node " tranactor")
              (when (transactor? test node)
                (db.transactor/install! test)
                (db.transactor/configure! test)
                (when (= node (jepsen/primary test))
                  (db.transactor/setup-dynamo! test)))))]
      ; We need the peer jar and transactor installed before we can start the
      ; transactor
      @peer-fut
      @transactor-fut
      (when (transactor? test node)
        (db/start! transactor test node))))

  (teardown! [this test node]
    (when (transactor? test node)
      (db/teardown! transactor test node))
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
  "Constructs a fresh Jepsen DB with a peer and transactor."
  [opts]
  (DB. (db.transactor/dynamo-db opts)
       (db.peer/db opts)))
