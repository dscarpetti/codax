(ns codax.operations
  (:require
   [codax.store :as store]
   [codax.pathwise :as pathwise]))

(defn collect [tx path])

(defn assoc-map [tx path v])

(defn update-map [tx path f & args])

(defn delete-map [tx path])

(defn search [tx path & {:keys [to-suffix limit reverse partial]}])

(defn put-map [tx path v])

(defn put-map-update [txn path f & args])

(defn get-val [tx path])

(defn put-val [tx path v])

(defn update-val [tx path f & args])

(defn delete-val [tx path f & args])
