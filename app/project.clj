(defproject jepsen.datomic.app "0.1.0-SNAPSHOT"
  :description "A small application that uses Datomic and exposes an HTTP API for Jepsen to talk to it"
  :url "https://github.com/jepsen-io/datomic"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"
                 [com.amazonaws/aws-java-sdk-dynamodb "1.11.600"]]]
  :repl-options {:init-ns jepsen.datomic.app})
