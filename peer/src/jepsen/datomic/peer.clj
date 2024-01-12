(ns jepsen.datomic.peer
  "A small peer app that exposes an HTTP service for Jepsen to talk to."
  (:gen-class)
  (:require [clojure [edn :as edn]
                     [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [datomic.api :as d]
            [jepsen.datomic.peer [append :as append]]
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
                  "/txn" (append/handle-txn conn body))]
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
    @(d/transact conn (concat append/schema))
    (start-server! conn)))
