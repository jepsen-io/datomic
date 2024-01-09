(ns jepsen.datomic.db
  "Automates the installation and teardown of Datomic clusters."
  (:require [clojure [edn :as edn]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [core :as jepsen]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def datomic-dir
  "Where do we download Datomic?"
  "/opt/datomic")

(def transactor-properties-file
  "The properties file for Datomic's transactor."
  (str datomic-dir "/transactor.properties"))

(def app-dir
  "Where do we download our little app?"
  "/opt/datomic-app")

(def aws-file
  "Where do we store AWS credentials locally?"
  "aws.edn")

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

(def aws
  "Returns AWS credentials from disk."
  (memoize #(edn/read-string (slurp aws-file))))

(defn datomic!
  "Runs a bin/datomic command with arguments, providing various env vars."
  [& args]
  (apply c/exec
         ; AWS credentials
         (c/lit (str "AWS_ACCESS_KEY_ID='" (:access-key-id (aws)) "'"))
         (c/lit (str "AWS_SECRET_KEY='" (:secret-key (aws)) "'"))
         ; Work around a bug where AWS SDK v1 is incompatible with Java 17
         ; See https://repost.aws/articles/ARPPEPfTPLTlGLHIsVVyyyYQ/troubleshooting-unable-to-unmarshall-exception-response-with-the-unmarshallers-provided-in-java
         (c/lit "DATOMIC_JAVA_OPTS='--add-opens java.base/java.lang=ALL-UNNAMED
                --add-opens java.xml/com.sun.org.apache.xpath.internal=ALL-UNNAMED'")
         "bin/datomic"
         args))

(defn setup-dynamo!
  "Sets up DynamoDB"
  []
  ; See https://docs.datomic.com/pro/overview/storage.html#automated-setup
  (c/cd datomic-dir
        (c/upload "resources/transactor.properties" transactor-properties-file)
        (datomic! :ensure-transactor transactor-properties-file transactor-properties-file)))

(defn install-app!
  "Installs our local application."
  []
  )

(defrecord DynamoDB []
  db/DB
  (setup! [this test node]
    (install-datomic! test)
    (setup-dynamo!)
    (install-app!))

  (teardown! [this test node]
    (c/su
      (c/exec :rm :-rf datomic-dir app-dir))))

(defn dynamo-db
  "Constructs a fresh DynamoDB-backed Datomic DB."
  [opts]
  (DynamoDB.))
