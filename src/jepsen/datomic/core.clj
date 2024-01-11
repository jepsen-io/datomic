(ns jepsen.datomic.core
  "Core constants and functions for all Datomic test namespaces.")

(def dynamo-table
  "What table do we use in DynamoDB?"
  "datomic-jepsen")

(def db-name
  "What database do we use in Datomic?"
  "jepsen")

(defn storage-uri
  "The URI for connecting to Datomic's storage. Needed for peers."
  ([]
   (storage-uri db-name))
  ([db-name]
   (str "datomic:ddb://us-east-1/" dynamo-table "/" db-name)))
