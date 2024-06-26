(ns jepsen.datomic.client
  "Client for our little Peer HTTP service"
  (:require [clojure [edn :as edn]
                     [string :as str]
                     [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [util :as util]]
            [org.httpkit.client :as http]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.httpkit ProtocolException)
           (org.httpkit.client TimeoutException)
           (java.io ByteArrayInputStream
                    InputStreamReader
                    PushbackReader)
           (java.net ConnectException
                     SocketException)))

(def port
  "What port does the peer's HTTP service listen on?"
  8000)

(defn path-fragment
  "Takes a path fragment for req! and returns a string. Named objects use their
  name, others are converted using str."
  [x]
  (if (instance? clojure.lang.Named x)
    (name x)
    (str x)))

(defn ensure-sequential
  "Takes a sequential collection or any x, and returns coll, or [x]."
  [coll-or-x]
  (if (sequential? coll-or-x)
    coll-or-x
    [coll-or-x]))

(defn read-stream
  "Reads a single EDN value from an inputstream."
  [stream]
  (with-open [r   (InputStreamReader. stream)
              pbr (PushbackReader. r)]
    (edn/read pbr)))

(defn req!
  "Makes a POST request for the given path (as a vector of
  strings/keywords/etc) with the given EDN-serializable body to the given node.
  Returns deserialized response, or throws."
  [node path body]
  (let [path (->> (ensure-sequential path)
                  (mapv path-fragment)
                  (str/join "/"))
        res @(http/post (str "http://" node ":" port "/" path)
                        {:body      (pr-str body)
                         :as        :stream
                         :timeout   2000
                         :keepalive -1})]
    ;(pprint res)
    (when-let [err (:error res)]
      (throw err))
    (when (<= 400 (:status res))
      ; Do we have an EDN body?
      (if-let [b (:body res)]
        (let [parsed (read-stream b)]
          (throw+ parsed))
        (throw+ res)))
    (read-stream (:body res))))

(defn await-open
  "Blocks until peer HTTP server is available."
  [node]
  (util/await-fn #(req! node :health nil)
                 {:log-message "Waiting for peer HTTP server..."
                  :log-interval 10000}))

(defmacro with-errors
  "Takes an op and body. Evaluates body, converting common errors to failed or
  info versions of op."
  [op & body]
  `(try+
     ~@body
     (catch ProtocolException e#
       (condp re-find (.getMessage e#)
         #"No status" (assoc ~op :type :info, :error [:no-status])
         (throw e#)))

     (catch ConnectException e#
       ; Slow down requests here so we don't spam down nodes super fast
       (Thread/sleep 1000)
       (assoc ~op :type :fail, :error [:conn-refused (.getMessage e#)]))

     (catch SocketException e#
       (condp re-find (.getMessage e#)
         #"Connection reset by peer"
         (assoc ~op :type :info, :error [:conn-reset-by-peer])

         #"Connection reset"
         (assoc ~op :type :info, :error [:conn-reset])

         (throw e#)))

     (catch TimeoutException e#
       (assoc ~op :type :info, :error [:http-timeout (.getMessage e#)]))

     (catch [:type :sync-timeout] e#
       (assoc ~op
              :type  (if (:definite? e#) :fail :info)
              :error [:sync-timeout]))

     (catch [:db/error :db.error/transaction-timeout] e#
       (assoc ~op :type :info, :error [:txn-timeout]))

     (catch [:cognitect.anomalies/category :cognitect.anomalies/incorrect] e#
       (assoc ~op
              :type :fail
              :error [:incorrect e#]))

     (catch [:cognitect.anomalies/category :cognitect.anomalies/conflict] e#
       (assoc ~op
              :type :fail
              :error [:conflict (:cognitect.anomalies/message e#)]))

     (catch [:cognitect.anomalies/category :cognitect.anomalies/unavailable] e#
       ;(info :unavailable (pr-str e#))
       ; Slow these down too
       (Thread/sleep 100)
       ;(info :unavailable-err (pr-str e#) (pr-str (:definite? e#)))
       (assoc ~op
              :type  (if (:definite? e#) :fail :info)
              :error [:unavailable (:cognitect.anomalies/message e#)]))

     (catch (when-let [m# (:message ~'%)]
              (re-find #"^:db\.error/cas-failed" m#)) e#
       (assoc ~op
              :type :fail
              :error [:cas-failed (:message e#)]))))
