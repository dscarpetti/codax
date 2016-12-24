(defproject codax "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.taoensso/nippy "2.12.2"]
                 [clj-time "0.12.0"]
                 [org.clojars.kliph/vijual "0.2.7"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.cache "0.6.5"]]
  :main ^:skip-aot codax.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
