(defproject codax "1.2.0-SNAPSHOT"
  :description "Codax is an idiomatic transactional embedded database for clojure"
  :url "https://github.com/dscarpetti/codax"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.taoensso/nippy "2.12.2"]
                 [clj-time "0.13.0"]
                 [org.clojars.kliph/vijual "0.2.7"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.cache "0.6.5"]]
  :main ^:skip-aot codax.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
