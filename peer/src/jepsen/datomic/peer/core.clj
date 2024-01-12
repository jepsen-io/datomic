(ns jepsen.datomic.peer.core
  "Core support functions for the Datomic peer."
  (:import (java.util List
                      Map
                      Set)))

(defn ->clj
  "Datomic serializes Clojure structures to Fressian, and then deserializes
  them to Java collections. This function recursively transforms Java Lists to
  vectors and Maps to maps. Useful when writing a transaction function that
  takes something more complex than a string or int."
  [x]
  (condp instance? x
    List (mapv ->clj x)
    Map  (persistent! (reduce (fn xform-pair [m [k v]]
                                (assoc! m (->clj k) (->clj v)))
                              (transient {})
                              x))
    Set  (into #{} (map ->clj x))
    x))
