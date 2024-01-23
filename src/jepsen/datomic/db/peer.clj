(ns jepsen.datomic.db.peer
  "A Jepsen Database which installs a small peer application from the local
  directory."
  (:require [clojure [edn :as edn]
                     [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java [io :as io]
                          [shell :refer [sh]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [core :as jepsen]
                    [fs-cache :as fs-cache]
                    [util :as util]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.datomic [aws :as aws]
                            [core :as dc]
                            [client :as client]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io File)
           (java.nio.file AtomicMoveNotSupportedException
                          StandardCopyOption
                          Files
                          Path)
           (java.nio.file.attribute FileAttribute)))

(def cleaned?
  "We use this to track whether we've cleaned the fs-cache copy of the uberjar
  during the current execution."
  (atom false))

(def local-dir
  "Where is the peer project directory locally?"
  "peer")

(def dir
  "Where do we install the peer application?"
  "/opt/datomic-peer")

(def jar
  "Where do we put the fat jar?"
  (str dir "/peer.jar"))

(def service
  "The systemd service name"
  "datomic-peer")

(def service-file
  "Where do we write the systemd service file?"
  (str "/etc/systemd/system/" service ".service"))

(def log-file
  "Stdout logs for the peer."
  (str dir "/peer.log"))

(def pid-file
  "Pidfile for the peer."
  (str dir "/peer.pid"))

(defn tar!
  "Creates a tarball of a local directory and sticks it in the FS cache under
  the given path. Returns the fs cache path."
  [local-dir fs-cache-path]
  (sh "tar" "czf"
      (.getAbsolutePath (fs-cache/file! fs-cache-path))
      local-dir)
  fs-cache-path)

(defn untar!
  "Takes a remote path to a tarball, and a remote directory, and untars it
  there. Returns remote directory."
  [remote-tarball remote-dir]
  (cu/install-archive! (str "file:///" remote-tarball) remote-dir))

(defn uberjar!
  "Builds a fat jar of the peer app and caches it. Returns cache path."
  [test]
  (let [path [:datomic :peer]]
    (fs-cache/locking path
      (when (or (and (:clean-peer test)
                     (not @cleaned?))
                (not (fs-cache/cached? path)))
        (info "Building peer jar")
        (let [env (-> (into {} (System/getenv))
                      (dissoc "CLASSPATH"))
              {:keys [out err exit] :as res}
              (sh "lein" "do" "clean," "uberjar" :dir "peer" :env env)]
          (when-not (zero? exit)
            (throw+ (assoc res :type :lein-clean-run-failed))))
        (let [uberjar (->> (file-seq (io/file "peer/target"))
                           (filter #(re-find #".+-standalone\.jar" (.getAbsolutePath ^File %)))
                           first)]
          (assert uberjar)
          (fs-cache/save-file! uberjar path)
          (reset! cleaned? true))))
    path))

(defn start!
  "Starts the peer app as a daemon."
  [test]
  (info "Starting peer")
  (c/su (c/exec :systemctl :start service)))

(def kill-pattern
  "A pattern matching the process for grepkill"
  (str "java.+ -jar " jar))

(defn stop!
  "Stops the peer app daemon."
  []
  (c/su
    (cu/grepkill! kill-pattern)
    (try+
      (c/exec :systemctl :stop service)
      :killed
      (catch [:exit 5] _
        :doesn't-exist))))

(defn install-jar!
  "Installs the peer jar."
  [test]
  (c/su
    (c/exec :mkdir :-p dir)
    (fs-cache/deploy-remote! (uberjar! test) jar)))

(defn install-service!
  "Installs the systemd service"
  [test]
  (c/su
    (-> (io/resource "peer.service")
        slurp
        (str/replace #"%DIR%" dir)
        (str/replace #"%LOG_FILE%" log-file)
        (str/replace #"%ENV%" (->> (aws/env)
                                   (map (fn [[k v]] (str
                                                      "Environment=\""
                                                      k "=" v "\"")))
                                   (str/join "\n")))
        (str/replace #"%EXEC_START%"
                     (str "/usr/bin/java"
                          " -server"
                          " -Djava.net.preferIPv4Stack=true"
                          ; See https://docs.datomic.com/pro/configuration/system-properties.html#peer-properties
                          " -Ddatomic.txTimeoutMsec=2000"
                          ; We use a smaller cache size to force Datomic to pull data from storage
                          ; more often
                          " -Ddatomic.objectCacheMax=" (:object-cache-max test)
                          " -jar " jar
                          " serve"
                          " " (dc/storage-uri)))
        (cu/write-file! service-file))
    (c/exec :systemctl :daemon-reload)))

(defrecord Peer []
  db/DB
  (setup! [this test node]
    (c/su
      (install-jar! test)
      (install-service! test)
      (start! test)
      (client/await-open node)))

  (teardown! [this test node]
    (c/su
      (stop!)
      (c/exec :rm :-rf dir)))

  db/LogFiles
  (log-files [this test node]
    {log-file "peer.log"})

  db/Kill
  (start! [this test node]
    (start! test))

  (kill! [this test node]
    (stop!))

  db/Pause
  (pause! [this test node]
    (c/su (cu/grepkill! :stop (str "java .+ -jar " jar))))

  (resume! [this test node]
    (c/su (cu/grepkill! :cont (str "java .+ -jar " jar)))))

(defn db
  "Constructs a new Peer DB"
  [opts]
  (Peer.))
