(ns codax.test-logging)

(def ^:dynamic *logging* false)

(defn logln [& args]
  (when *logging* (apply println args)))
