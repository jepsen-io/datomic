(ns jepsen.datomic.peer
  "A small peer app that exposes an HTTP service for Jepsen to talk to."
  (:gen-class)
  (:require [clojure [edn :as edn]
                     [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [datomic.api :as d]
            [jepsen.datomic.peer [append :as append]
                                 [append-cas :as append-cas]
                                 [grant :as grant]
                                 [internal :as internal]]
            [org.httpkit.server :as http-server]
            [slingshot.slingshot :refer [try+ throw+]]
            [unilog.config :refer [start-logging!]])
  (:import (java.io InputStreamReader
                    PushbackReader)
           (java.util Date)
           (java.util.concurrent ExecutionException)
           (clojure.lang ExceptionInfo)))

(def port
  "What port do we bind our HTTP interface to?"
  8000)

(defn await-fn
  "Invokes a function (f) repeatedly. Blocks until (f) returns, rather than
  throwing. Returns that return value. Catches Exceptions (except for
  InterruptedException) and retries them automatically. Options:

    :retry-interval   How long between retries, in ms. Default 1s.
    :log-interval     How long between logging that we're still waiting, in ms.
                      Default `retry-interval.
    :log-message      What should we log to the console while waiting?
    :timeout          How long until giving up and throwing :type :timeout, in
                      ms. Default 60 seconds."
  ([f]
   (await-fn f {}))
  ([f opts]
   (let [log-message    (:log-message opts (str "Waiting for " f "..."))
         retry-interval (:retry-interval opts 1000)
         log-interval   (:log-interval opts retry-interval)
         timeout        (:timeout opts 60000)
         t0             (System/nanoTime)
         log-deadline   (atom (+ t0 (* 1e6 log-interval)))
         deadline       (+ t0 (* 1e6 timeout))]
     (loop []
       (let [res (try
                   (f)
                   (catch InterruptedException e
                     (throw e))
                   (catch Exception e
                     ;(warn e "retry")
                     (let [now (System/nanoTime)]
                       ; Are we out of time?
                       (when (<= deadline now)
                         (throw+ {:type :timeout} e))

                       ; Should we log something?
                       (when (<= @log-deadline now)
                         (info log-message)
                         (swap! log-deadline + (* log-interval 1e6)))

                       ; Right, sleep and retry
                       (Thread/sleep retry-interval)
                       ::retry)))]
         (if (= ::retry res)
           (recur)
           res))))))

(defn create-database!
  "Creates database."
  [uri]
  (try
    (d/create-database uri)
    :done
    (catch Exception e
      ;(warn e "Couldn't create database")
      (throw e))))

(defmacro unwrap-ee
  "Datomic throws ExecutionExceptions wrapping the actual error, which means we
  can't do try/catch dispatch on exception types. This macro evaluates body,
  catches EEs, and rethrows their causes. Doing this causes us to lose the
  stacktrace of the caller though, so we log the full exception here."
  [& body]
  `(try
     ~@body
     (catch ExecutionException e#
       (warn e# "Error during request handling")
       (throw (.getCause e#)))))

(defn make-http-app
  "Takes a Datomic connection and returns a Ring app: a function which takes
  HTTP requests."
  [conn]
  (fn app [req]
    (try
      (unwrap-ee
        ;(info (with-out-str (pprint req)))
        (let [body (with-open [r   (InputStreamReader. (:body req))
                               pbr (PushbackReader. r)]
                     (edn/read pbr))
              res (case (:uri req)
                    "/gc"     (let [r (d/gc-storage conn (Date.))]
                                [:ok r])
                    "/grant"    (grant/handle-txn conn body)
                    "/health"   :ok
                    "/internal" (internal/handle-txn conn body)
                    "/stats"    (d/db-stats (d/db conn))
                    "/txn"      (append/handle-txn conn body)
                    "/txn-cas"  (append-cas/handle-txn conn body))]
          {:status  200
           :headers {"Content-Type" "application/edn"}
           :body    (pr-str res)}))
      (catch datomic.impl.Exceptions$IllegalArgumentExceptionInfo e
        {:status  500
         :headers {"Content-Type" "application/edn"}
         :body    (pr-str (ex-data e))})
      (catch clojure.lang.ExceptionInfo e
        {:status  500
         :headers {"Content-Type" "application/edn"}
         :body    (pr-str (ex-data e))})
      (catch Exception e
        (warn e "Error during request handling")
        {:status  500
         :headers {"Content-Type" "application/edn"}
         :body    (pr-str {:definite? false
                           :type      (.getName (class e))
                           :message   (.getMessage e)})}))))

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

(defn self-test
  "Runs a simple self-test. Helpful for debugging. You probably want to run the
  test once to set up Dynamo etc, grab the full command to run the jar from the
  top of store/latest/n3/peer.log, and then run that with `test` instead of
  `serve`."
  [conn]
  ;(info "Append")
  ;(d/transact conn [[:db/retractEntity [:append-cas/key 0]]])
  ;(info (append-cas/handle-txn conn {:txn [[:append 0 1]]}))
  ;(info nil)
  ;(info (append-cas/handle-txn conn {:txn [[:r 0 nil]]}))
  ;(info nil)
  ;(info (append-cas/handle-txn conn {:txn [[:r 0 nil] [:append 0 2] [:r 0 nil] [:append 0 3]]}))
  ;(info nil)
  ;(info (append-cas/handle-txn conn {:txn [[:r 0 nil]]}))

  (info "Grant")
  ;(info :all (grant/all-ids @(d/sync conn)))
  @(d/transact conn [['jepsen.datomic.peer.grant/reset]])
  (let [r  (grant/handle-txn conn {:txn [['jepsen.datomic.peer.grant/create]]})
        id (:db/id (first (:state' r)))]
    (info :r r)
    ;(info :state' (:state' r))
    ;(info :id id)
    (info (grant/handle-txn
            conn
            {:txn [['jepsen.datomic.peer.grant/approve id]
                   ['jepsen.datomic.peer.grant/deny id]]}))
  ))

(defn -main
  "CLI entry point. Two commands:

  serve - Runs a web server that takes Jepsen requests and executes them
  test  - For local development, simulates a simple request"
  [cmd uri]
  (start-logging! {:console true
                   :level "info"
                   :overrides {"datomic.peer"            :warn
                               "datomic.kv-cluster"      :warn
                               "datomic.log"             :warn
                               "datomic.domain"          :warn
                               "datomic.process-monitor" :warn}})
  (info "Starting peer:" uri)
  (await-fn (partial create-database! uri)
            {:log-message "Ensuring DB exists"
             :timeout     Long/MAX_VALUE})
  (let [conn (await-fn (partial d/connect uri)
                       {:log-message "Connecting to Datomic"
                        :timeout Long/MAX_VALUE})]
    ; Create schema
    (await-fn #(deref (d/transact conn (concat append/schema
                                               append-cas/schema
                                               grant/schema
                                               internal/schema)))
              {:log-message "Ensuring schema"
               :timeout     Long/MAX_VALUE})
    (case cmd
      "serve" (start-server! conn)
      "test"  (self-test conn))
    (shutdown-agents)
    (System/exit 0)))
