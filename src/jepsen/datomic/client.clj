(ns jepsen.datomic.client
  "Client for our little Peer HTTP service"
  (:require [clojure [edn :as edn]
                     [string :as str]
                     [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [util :as util]]
            [org.httpkit.client :as http]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io InputStreamReader
                    PushbackReader)
           (java.net ConnectException)))

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
                        {:body (pr-str body)
                         :as :stream})]
    ;(pprint res)
    (when-let [err (:error res)]
      (throw err))
    (when (<= 400 (:status res))
      ; Do we have an EDN body?
      (if-let [b (:body res)]
        (throw+ (read-stream b))
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
     (catch ConnectException e#
       (assoc ~op :type :fail, :error :conn-refused))))