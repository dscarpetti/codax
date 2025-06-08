(defproject codax "1.4.3"
  :description "Codax is an idiomatic transactional embedded database for clojure"
  :url "https://github.com/dscarpetti/codax"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.taoensso/nippy "3.2.0"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.cache "0.6.5"]]
  :target-path "target/%s"
  :profiles {:uberjar {}
             :dev {:dependencies [[org.clojars.kliph/vijual "0.2.7"]]
                   :source-paths ["dev"]}})
