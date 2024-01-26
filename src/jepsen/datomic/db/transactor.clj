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
            [jepsen.datomic.db [datomic :as datomic :refer [dir
                                                            properties-file]]
                               [peer :as peer]
                               [storage :as storage]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

(def service
  "The systemd service name"
  "datomic-transactor")

(def service-file
  "Where we put the systemd unit file"
  (str "/etc/systemd/system/" service ".service"))

(def pid-file
  "The pidfile for the transactor daemon."
  (str dir "/transactor.pid"))

(def log-file
  "The stdout logfile for the transactor."
  (str dir "/transactor.log"))

(defn install!
  "Installs the transactor service file."
  [test]
  (c/su
    (info "Installing transactor")
    (-> (io/resource "transactor.service")
        slurp
        (str/replace #"%DIR%" dir)
        (str/replace #"%LOG_FILE%" log-file)
        (str/replace #"%ENV%"
                     (->> (datomic/env)
                          ; Add our custom txn fns to the classpath
                          (merge {"DATOMIC_EXT_CLASSPATH" peer/jar})
                          (map (fn [[k v]]
                                 (str "Environment=\"" k "=" v "\"")))
                          (str/join "\n")))
        (str/replace #"%EXEC_START%"
                     (str dir "/bin/transactor "
                          "-Ddatomic.printconnectionInfo=true "
                          properties-file))
        (cu/write-file! service-file))
    (c/exec :systemctl :daemon-reload)))

(defn configure!
  "Writes initial config file"
  [test storage]
  (c/su
    (info "Configuring transactor.properties")
    (-> (io/resource "transactor.properties")
        slurp
        (str/replace #"%STORAGE%" (storage/properties-snippet storage test))
        (str/replace #"%HOST%" (cn/local-ip))
        (str/replace #"%OBJECT_CACHE_MAX%"       (:object-cache-max test))
        (str/replace #"%MEMORY_INDEX_THRESHOLD%" (:memory-index-threshold test))
        (str/replace #"%MEMORY_INDEX_MAX%"       (:memory-index-max test))
        (cu/write-file! properties-file))))

(defn start!
  "Launches the transactor daemon."
  []
  (info "Starting transactor")
  (c/su (c/exec :systemctl :start service))
  :started)

(def kill-pattern
  "A pattern matching the process for grepkill"
  "^java.+transactor\\.properties")

(defn stop!
  "Kills the transactor daemon."
  []
  (info "Killing transactor...")
  (c/su
    (cu/grepkill! kill-pattern)
    (try+
      (c/exec :systemctl :stop service)
      :killed
      (catch [:exit 5] _
        :doesn't-exist))))

(defrecord Transactor [storage peer]
  db/DB
  (setup! [this test node]
    (install! test)
    (configure! test storage)
    ; We need storage available
    (db/setup! storage test node)
    ; And before we can start, we need the peer jar installed
    (peer/install-jar! test)
    ; Now we can start
    (start!))

  (teardown! [this test node]
    (stop!)
    (db/teardown! storage test node))

  db/LogFiles
  (log-files [this test node]
    (merge {log-file "transactor.log"}
           (try+ (->> (cu/ls-full (str dir "/log"))
                      (map-indexed (fn [i file]
                                     [file (str "transactor-" i ".log")]))
                      (into {}))
                 (catch [:exit 2] _))))

  db/Kill
  (start! [this test node]
    (start!))

  (kill! [this test node]
    (stop!))

  db/Pause
  (pause! [this test node]
    (c/su (cu/grepkill! :stop kill-pattern)))

  (resume! [this test node]
    (c/su (cu/grepkill! :cont kill-pattern))))

(defn db
  "Constructs a fresh transactor given CLI options and Peer and Storage DBs.
  The transactor manages storage's setup and teardown."
  [opts storage peer]
  (Transactor. storage peer))
