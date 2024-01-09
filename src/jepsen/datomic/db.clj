(ns jepsen.datomic.db
  "Automates the installation and teardown of Datomic clusters."
  (:require [clojure [edn :as edn]
                     [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as aws.creds]
            [jepsen [control :as c]
                    [db :as db]
                    [core :as jepsen]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

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

(def dynamo-table
  "What table do we use in DynamoDB?"
  "datomic-jepsen")

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

(defn aws-client
  "Constructs a new AWS client for the given API."
  [api]
  (aws/client {:api api
               :credentials-provider
               (aws.creds/basic-credentials-provider
                 {:access-key-id     (:access-key-id (aws))
                  :secret-access-key (:secret-key (aws))})}))

(defn aws-invoke
  "Like aws/invoke, but throws exceptions on errors. I... I have questions."
  [client op]
  (let [r (aws/invoke client op)]
    (if (contains? r :cognitect.anomalies/category)
      (throw+ (assoc r :type :aws-error))
      r)))

(defn aws-list-dynamo
  []
  (with-open [ddb (aws-client :dynamodb)]
    (pprint (aws/ops ddb))))

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
    ; Delete AWS resources
    ; Dynamo table
    (try+ (let [d (aws-client :dynamodb)
                r (aws-invoke d {:op :DeleteTable
                                 :request {:TableName dynamo-table}})]
            (assert (= "DELETING" (:TableStatus (:TableDescription r)))
                    (pr-str r)))
          (catch [:cognitect.aws.error/code "ResourceNotFoundException"] _))

    ; IAM roles. The ensure-dynamo command might create a whole bunch of these
    ; over multiple runs, and we can't make sure we cleaned them all up
    ; properly, so we delete every one. See
    ; https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_manage_delete.html
    (let [i (aws-client :iam)
          roles (->> (aws/invoke i {:op :ListRoles})
                     :Roles
                     (map :RoleName)
                     (filter (partial re-find #"^datomic-aws-")))]
      (doseq [role roles]
        ; Profiles
        (let [profiles (->> (aws-invoke i {:op :ListInstanceProfilesForRole
                                           :request {:RoleName role}})
                            :InstanceProfiles
                            (map :InstanceProfileName))]
          (doseq [profile profiles]
            (aws-invoke i {:op :RemoveRoleFromInstanceProfile
                           :request {:InstanceProfileName profile
                                     :RoleName role}})
            (info "Deleting instance profile" profile)
            (aws-invoke i {:op :DeleteInstanceProfile
                           :request {:InstanceProfileName profile}})))

        ; Inline policies
        (let [policies (->> (aws-invoke i {:op :ListRolePolicies
                                           :request {:RoleName role}})
                            :PolicyNames)]
          (doseq [policy policies]
            (info "Deleting inline role policy" policy)
            (aws-invoke i {:op :DeleteRolePolicy
                           :request {:RoleName role
                                     :PolicyName policy}})))

        ; Role itself
        (info "Deleting role" role)
        (aws-invoke i {:op :DeleteRole
                       :request {:RoleName role}})))
    (c/su
      (c/exec :rm :-rf datomic-dir app-dir))))

(defn dynamo-db
  "Constructs a fresh DynamoDB-backed Datomic DB."
  [opts]
  (DynamoDB.))
