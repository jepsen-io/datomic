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
            [jepsen.datomic.db [peer :as db.peer]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

(def datomic-dir
  "Where do we download Datomic?"
  "/opt/datomic")

(def transactor-properties-file
  "The properties file for Datomic's transactor."
  (str datomic-dir "/transactor.properties"))

(def transactor-pid-file
  "The pidfile for the transactor daemon."
  (str datomic-dir "/transactor.pid"))

(def transactor-log-file
  "The stdout logfile for the transactor."
  (str datomic-dir "/transactor.log"))

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

(defn install-datomic!
  "Installs the Datomic distribution."
  [test]
  (c/su
    ; We need Java
    (debian/install ["openjdk-17-jdk-headless"])
    ; Install Datomic itself
    (cu/install-archive!
      (str "https://datomic-pro-downloads.s3.amazonaws.com/"
           (:version test) "/datomic-pro-" (:version test) ".zip")
      datomic-dir)))

(defn datomic-env
  "Constructs an env var map for bin/transactor, bin/datomic, etc."
  []
  (merge (aws/env)
         {; Work around a bug where AWS SDK v1 is incompatible with Java 17
          ; See https://repost.aws/articles/ARPPEPfTPLTlGLHIsVVyyyYQ/troubleshooting-unable-to-unmarshall-exception-response-with-the-unmarshallers-provided-in-java
          "DATOMIC_JAVA_OPTS" (str "--add-opens java.base/java.lang=ALL-UNNAMED "
                                   "--add-opens java.xml/com.sun.org.apache.xpath.internal=ALL-UNNAMED")
          ; Add our custom txn fns to the classpath
          "DATOMIC_EXT_CLASSPATH" db.peer/jar}))

(defn datomic!
  "Runs a bin/datomic command with arguments, providing various env vars."
  [& args]
  (apply c/exec (c/env (datomic-env)) "bin/datomic" args))

(defn configure!
  "Writes initial config file"
  []
  (c/su
    (-> (io/resource "transactor.properties")
        slurp
        (str/replace #"%HOST%" (cn/local-ip))
        (cu/write-file! transactor-properties-file))))

(defn setup-dynamo!
  "Sets up DynamoDB, creating initial tables and IAM roles/policies"
  []
  ; See https://docs.datomic.com/pro/overview/storage.html#automated-setup
  (c/cd datomic-dir
        (datomic! :ensure-transactor transactor-properties-file transactor-properties-file)))

(defn start-transactor!
  "Launches the transactor daemon."
  []
  (cu/start-daemon!
    {:chdir   datomic-dir
     :logfile transactor-log-file
     :pidfile transactor-pid-file
     :env     (datomic-env)}
    "bin/transactor"
    "-Ddatomic.printConnectionInfo=true"
    transactor-properties-file))

(defn stop-transactor!
  "Kills the transactor daemon."
  []
  (cu/stop-daemon! transactor-pid-file))

(defrecord DynamoDB [peer]
  db/DB
  (setup! [this test node]
    (let [peer (future
                 (with-thread-name (str "jepsen node " node " peer")
                   (cond (peer? test node)
                         (db/setup! peer test node)

                         ; We still want a copy of the peer lib on the
                         ; classpath so we can call its fns in transactions.
                         (transactor? test node)
                         (db.peer/install! test))))]
      (install-datomic! test)
      (when (transactor? test node)
        (configure!)
        (when (= node (jepsen/primary test) )
          (setup-dynamo!))
        @peer ; Make sure classpath is ready
        (start-transactor!))
      @peer))

  (teardown! [this test node]
    (when (transactor? test node)
      (stop-transactor!))
    (when (peer? test node)
      (db/teardown! peer test node))
    (when (= node (jepsen/primary test))
      (aws/delete-all!))
    (c/su
      (c/exec :rm :-rf datomic-dir)))

  db/LogFiles
  (log-files [this test node]
    (merge {transactor-log-file "transactor.log"}
           (try+ (->> (cu/ls-full (str datomic-dir "/log"))
                      (map-indexed (fn [i file]
                                     [file (str "transactor-" i ".log")]))
                      (into {}))
                 (catch [:exit 2] _))
           (db/log-files peer test node))))

(defn dynamo-db
  "Constructs a fresh DynamoDB-backed Datomic DB."
  [opts]
  (DynamoDB. (db.peer/db opts)))
