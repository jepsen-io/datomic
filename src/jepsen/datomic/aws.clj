(ns jepsen.datomic.aws
  "Helpers for working with AWS stuff."
    (:require [clojure [edn :as edn]
                       [pprint :refer [pprint]]]
              [clojure.tools.logging :refer [info warn]]
              [cognitect.aws.client.api :as aws]
              [cognitect.aws.credentials :as aws.creds]
              [jepsen.datomic [core :as dc]]
              [slingshot.slingshot :refer [try+ throw+]]))

(def aws-file
  "Where do we store AWS credentials locally?"
  "aws.edn")

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

(defn delete-dynamo-table!
  "Deletes the dynamo table we use."
  []
  (info "Deleting Dynamo table" dc/dynamo-table)
  (try+ (let [d (aws-client :dynamodb)
              r (aws-invoke d {:op :DeleteTable
                               :request {:TableName dc/dynamo-table}})]
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

(defn delete-all!
  "Cleans up all Datomic-related AWS resources."
  []
  (delete-dynamo-table!)
  (delete-iam-roles!))