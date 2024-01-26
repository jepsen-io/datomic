(ns jepsen.datomic.db.datomic
  "Automates the installation and teardown of the Datomic library. This is the
  bottom layer of a dependency sandwich: db.transactor -> db.storage ->
  db.datomic."
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
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

(def dir
  "Where do we download Datomic's packages?"
  "/opt/datomic")

(def properties-file
  "The properties file for Datomic's transactor. A bit awkward: this sort of
  belongs more in db.transactor, but db.storage needs it, and db.transactor
  depends on storage, so... "
  (str dir "/transactor.properties"))

(defn env
  "Constructs an env var map for bin/transactor, bin/datomic, etc."
  []
  (merge (aws/env)
         {; Work around a bug where AWS SDK v1 is incompatible with Java 17
          ; See https://repost.aws/articles/ARPPEPfTPLTlGLHIsVVyyyYQ/troubleshooting-unable-to-unmarshall-exception-response-with-the-unmarshallers-provided-in-java
          "DATOMIC_JAVA_OPTS" (str "--add-opens java.base/java.lang=ALL-UNNAMED "
                                   "--add-opens java.xml/com.sun.org.apache.xpath.internal=ALL-UNNAMED")}))

(defn datomic!
  "Runs a bin/datomic command with arguments, providing various env vars."
  [& args]
  (c/su
    (c/cd dir
          (apply c/exec (c/env (env)) "bin/datomic" args))))


(defrecord Datomic []
  db/DB
  (setup! [this test node]
    (c/su
      (info "Installing Datomic package")
      ; Install Datomic itself
      (cu/install-archive!
        (str "https://datomic-pro-downloads.s3.amazonaws.com/"
             (:version test) "/datomic-pro-" (:version test) ".zip")
        dir)))

  (teardown! [this test node]
    (c/su
      (c/exec :rm :-rf dir))))

(defn db
  "Constructs a Datomic Jepsen DB from CLI options."
  [opts]
  (Datomic.))
