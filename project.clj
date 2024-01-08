(defproject jepsen.datomic "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the Datomic distributed database"
  :url "https://github.com/jepsen-io/datomic"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5-SNAPSHOT"]]
  :main jepsen.datomic.cli
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"]
  :repl-options {:init-ns jepsen.datomic.cli})
