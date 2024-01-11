(ns jepsen.datomic.peer
  "A small peer app that exposes an HTTP service for Jepsen to talk to."
  (:gen-class)
  (:require [clojure [edn :as edn]
                     [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [datomic.api :as d]
            [org.httpkit.server :as http-server]
            [slingshot.slingshot :refer [try+ throw+]]
            [unilog.config :refer [start-logging!]])
  (:import (java.io InputStreamReader
                    PushbackReader)
           (java.util.concurrent ExecutionException)
           (clojure.lang ExceptionInfo)))

(def port
  "What port do we bind our HTTP interface to?"
  8000)

(defn create-database!
  "Creates database in a loop, retrying when the transactors aren't ready yet."
  [uri]
  (loop [backoff 10] ; ms
    (info "Creating database" uri)
    (let [r (try (d/create-database uri)
                 (catch java.lang.IllegalArgumentException e
                   (if (re-find #":db\.error/read-transactor-location-failed" (.getMessage e))
                     :retry
                     (throw e))))]
      (when (= r :retry)
        (do (Thread/sleep backoff)
            (recur (min 5000 (* 2 backoff))))))))

(def schema
  [{:db/ident       :append/key
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "The unique identifier for an append workload list."}
   {:db/ident       :append/elements
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/many
    :db/doc         "A single element in an append workload"}])

(defn handle-txn
  "Handles a txn request"
  [conn txn]
  ; A basic, wrong transactional interpreter
  (mapv (fn [[f k v :as mop]]
          (case f
            :r
            (let [db (d/db conn)
                  elements (d/q '{:find  [?element]
                                  :in    [$ ?k]
                                  :where [[?list :append/key ?k]
                                          [?list :append/elements ?element]]}
                                db
                                k)]
              [f k (mapv first elements)])

            :append
            (let [r @(d/transact conn
                                 [{:append/key      k
                                   :append/elements v}])]
              (info :res r)
              mop)))
        txn))

(defn make-http-app
  "Takes a Datomic connection and returns a Ring app: a function which takes
  HTTP requests."
  [conn]
  (fn app [req]
    (try
      (info (with-out-str (pprint req)))
      (let [body (with-open [r   (InputStreamReader. (:body req))
                             pbr (PushbackReader. r)]
                   (edn/read pbr))
            res (case (:uri req)
                  "/health" :ok
                  "/txn" (handle-txn conn body))]
        {:status  200
         :headers {"Content-Type" "application/edn"}
         :body    (pr-str res)})
      (catch ExecutionException e
        (warn e "Error during request handling")
        (throw (.getCause e)))
      (catch ExceptionInfo e
        {:status  500
         :headers {"Content-Type" "application/edn"}
         :body    (pr-str {:type    (str (class e))
                           :message (.getMessage e)
                           :data    (pr-str (ex-info e))})})
      (catch Exception e
        (warn e "Error during request handling")
        {:status  500
         :headers {"Content-Type" "application/edn"}
         :body    (pr-str {:type    (str (class e))
                           :message (.getMessage e)})}))))

(defn start-server!
  "Starts the HTTP server, given a Datomic connection."
  [conn]
  (let [shutdown (http-server/run-server
                   (make-http-app conn)
                   {:port port
                    :min-thread-count 4
                    :max-thread-count 128})]
    (info (str "Listening on http://0.0.0.0:" port))
    (while true
      (Thread/sleep 1000))))

(defn -main
  "CLI entry point"
  [uri]
  (start-logging! {:console true
                   :level "info"})
  (info "Starting peer 1")
  (create-database! uri)
  (let [conn (d/connect uri)]
    @(d/transact conn schema)
    (start-server! conn)))
