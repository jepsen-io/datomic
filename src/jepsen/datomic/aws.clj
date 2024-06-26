(ns jepsen.datomic.aws
  "Helpers for working with AWS stuff."
    (:require [clojure [edn :as edn]
                       [pprint :refer [pprint]]]
              [clojure.java [io :as io]]
              [clojure.tools.logging :refer [info warn]]
              [cognitect.aws.client.api :as aws]
              [cognitect.aws.credentials :as aws.creds]
              [dom-top.core :refer [with-retry]]
              [jepsen.util :refer [await-fn]]
              [jepsen.datomic [core :as dc]]
              [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io File)))

(def aws-file-name
  "Where do we store AWS credentials locally?"
  "aws.edn")

(def aws
  "Returns AWS credentials from disk."
  (memoize #(let [aws-file (io/file aws-file-name)]
              (when (.exists ^File aws-file)
                (edn/read-string (slurp aws-file-name))))))

(defn env
  "Returns an env var map for invoking Datomic JVM apps"
  []
  (when-let [basic-creds (aws)]
    {"AWS_ACCESS_KEY_ID" (:access-key-id basic-creds)
     "AWS_SECRET_KEY"    (:secret-key basic-creds)}))

(defn aws-client
  "Constructs a new AWS client for the given API, optionally
  using local AWS credentials if they exist."
  [api]
  (let [basic-creds (aws)]
    (aws/client (cond-> {:api api}
                        basic-creds (assoc :credentials-provider
                                           (aws.creds/basic-credentials-provider
                                             {:access-key-id     (:access-key-id basic-creds)
                                              :secret-access-key (:secret-key basic-creds)}))))))

(defn aws-invoke
  "Like aws/invoke, but throws exceptions on errors. I... I have questions."
  [client op]
  (let [r (aws/invoke client op)]
    (if (contains? r :cognitect.anomalies/category)
      (throw+ (assoc r :type :aws-error))
      r)))

(defn provision-io!
  "Adjusts the provisioned throughput settings of the Datomic Dynamo table to
  match the test."
  [test table]
  (await-fn
    (fn []
      (let [d (aws-client :dynamodb)
            r (aws-invoke
                      d
                      {:op :UpdateTable
                       :request
                       {:TableName table
                        :ProvisionedThroughput
                        {:ReadCapacityUnits (:dynamo-read-capacity test)
                         :WriteCapacityUnits (:dynamo-write-capacity test)}}})]
        r))
    {:retry-interval 5000
     :log-message (str "Waiting for Dynamo table " table
                       " to be created so we can provision it")
     :log-interval 10000}))

(defn delete-dynamo-table!
  "Deletes the dynamo table we use."
  [table]
  (info "Deleting Dynamo table" table)
  (try+ (let [d (aws-client :dynamodb)
              r (aws-invoke d {:op :DeleteTable
                               :request {:TableName table}})]
          (assert (= "DELETING" (:TableStatus (:TableDescription r)))
                  (pr-str r)))
        (catch [:cognitect.aws.error/code "ResourceNotFoundException"] _)))

(defn delete-iam-roles!
  "Cleans up all IAM roles. The ensure-dynamo command might create a whole
  bunch of these over multiple runs, and we can't make sure we cleaned them
  all up properly, so we delete every one. See
  https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_manage_delete.html"
  []
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
                     :request {:RoleName role}}))))

(defn delete-all*!
  "Cleans up all Datomic-related AWS resources."
  [table]
  (delete-dynamo-table! table)
  (delete-iam-roles!))

(defn delete-all!
  "Takes a Dynamo table name. Cleans up all Datomic-related AWS resources.
  Retries ResourceInUseExceptions."
  [table]
  (with-retry [attempts 30]
    (try+
      (delete-all*! table)
      (catch [:cognitect.aws.error/code "ResourceInUseException"] e
        (when (zero? attempts)
          (throw+ e))
        (info "Resource in use, retrying...")
        (Thread/sleep 2000)
        (retry (dec attempts))))))
