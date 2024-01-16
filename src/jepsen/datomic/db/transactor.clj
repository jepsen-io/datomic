(ns jepsen.datomic.db.transactor
  "Automates the installation and teardown of Datomic transactors."
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

(def dir
  "Where do we download Datomic's packages?"
  "/opt/datomic")

(def properties-file
  "The properties file for Datomic's transactor."
  (str dir "/transactor.properties"))

(def pid-file
  "The pidfile for the transactor daemon."
  (str dir "/transactor.pid"))

(def log-file
  "The stdout logfile for the transactor."
  (str dir "/transactor.log"))

(defn install!
  "Installs the Datomic distribution."
  [test]
  (c/su
    ; Install Datomic itself
    (cu/install-archive!
      (str "https://datomic-pro-downloads.s3.amazonaws.com/"
           (:version test) "/datomic-pro-" (:version test) ".zip")
      dir)))

(defn env
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
  (c/cd dir
        (apply c/exec (c/env (env)) "bin/datomic" args)))

(defn configure!
  "Writes initial config file"
  [test]
  (c/su
    (-> (io/resource "transactor.properties")
        slurp
        (str/replace #"%HOST%" (cn/local-ip))
        (cu/write-file! properties-file))))

(defn setup-dynamo!
  "Sets up DynamoDB, creating initial tables and IAM roles/policies"
  [test]
  ; See https://docs.datomic.com/pro/overview/storage.html#automated-setup
  (datomic! :ensure-transactor properties-file properties-file)
  (aws/provision-io! test))

(defn start!
  "Launches the transactor daemon."
  []
  (cu/start-daemon!
    {:chdir   dir
     :logfile log-file
     :pidfile pid-file
     :env     (env)}
    "bin/transactor"
    "-Ddatomic.printConnectionInfo=true"
    properties-file))

(defn stop!
  "Kills the transactor daemon."
  []
  (cu/stop-daemon! pid-file))

(defrecord DynamoDB [peer]
  db/DB
  (setup! [this test node]
    ; Handled by jepsen.datomic.db
    )

  (teardown! [this test node]
    (stop!)
    (when (= node (jepsen/primary test))
      (aws/delete-all!))
    (c/su
      (c/exec :rm :-rf dir)))

  db/LogFiles
  (log-files [this test node]
    (merge {log-file "transactor.log"}
           (try+ (->> (cu/ls-full (str dir "/log"))
                      (map-indexed (fn [i file]
                                     [file (str "transactor-" i ".log")]))
                      (into {}))
                 (catch [:exit 2] _)))))

(defn dynamo-db
  "Constructs a fresh DynamoDB-backed transactor DB."
  [opts]
  (DynamoDB. (db.peer/db opts)))
