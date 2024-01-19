(defproject jepsen.datomic "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the Datomic distributed database"
  :url "https://github.com/jepsen-io/datomic"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[com.cognitect.aws/api "0.8.686"]
                 [com.cognitect.aws/endpoints "1.1.12.504"]
                 [com.cognitect.aws/iam "848.2.1413.0"]
                 [com.cognitect.aws/kms "848.2.1413.0"]
                 [com.cognitect.aws/dynamodb "848.2.1413.0"]
                 [org.clojure/clojure "1.11.1"]
                 [cheshire "5.12.0"]
                 [http-kit "2.7.0"]
                 [jepsen "0.3.5-SNAPSHOT"]
                 [slingshot "0.12.2"]]
  :main jepsen.datomic.cli
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"]
  :repl-options {:init-ns jepsen.datomic.cli})
