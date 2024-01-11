(defproject jepsen.datomic.peer "0.1.0-SNAPSHOT"
  :description "A small peer application that uses Datomic and exposes an HTTP API for Jepsen to talk to it"
  :url "https://github.com/jepsen-io/datomic"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.logging "1.2.4"]
                 [com.amazonaws/aws-java-sdk-dynamodb "1.12.564"]
                 ; TODO: draw this from the CLI version?
                 [com.datomic/peer "1.0.7075"
                  :exclusions [org.slf4j/slf4j-nop org.slf4j/slf4j-log4j12]]
                 [ch.qos.logback/logback-classic "1.0.1"]
                 [http-kit "2.7.0"]
                 [spootnik/unilog "0.7.31"]
                 [slingshot "0.12.2"]]
  :repl-options {:init-ns jepsen.datomic.peer}
  :main jepsen.datomic.peer
  :profiles {:uberjar {:aot  :all
                       :main jepsen.datomic.peer}})
