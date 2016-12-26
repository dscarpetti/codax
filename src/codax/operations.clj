(ns codax.operations
  (:require
   [codax.store :as store]
   [codax.pathwise :as pathwise]))


(defn get-val [tx path]
  (store/b+get tx (pathwise/encode path)))

(defn delete-val [tx path]
  (store/b+remove tx (pathwise/encode path)))

(defn put-val [tx path v]
  (store/b+insert tx (pathwise/encode path) v))

(defn update-val [tx path f & args]
  (put-val tx path (apply f (get-val tx path) args)))

(defn seek
  ([tx] (seek tx nil nil))
  ([tx full-path] (seek tx full-path full-path))
  ([tx full-start-path full-end-path & {:keys [limit reverse keys-only no-decode]}]
   (let [start (if (empty? full-start-path)
                 (str (char 0x00))
                 (str (pathwise/partially-encode full-start-path) (char 0x00)))
         end (if (empty? full-end-path)
               (str (char 0xff))
               (str (pathwise/partially-encode full-end-path) (char 0x00) (char 0xff)))
         results (store/b+seek tx start end :limit limit :reverse reverse)]
     (cond
       (and no-decode keys-only) (map first results)
       no-decode results
       keys-only (map #(pathwise/decode (first %)) results)
       :else (map #(update % 0 pathwise/decode) results)))))

(defn- reduce-assoc [coll [k v]]
  (if (not (vector? k))
    coll
    (assoc-in coll k v)))

(defn- complete-collection [tx full-path coll]
  (get-in
   (if (:write tx)
     (let [path-length (.length full-path)]
       (reduce reduce-assoc
               coll
               (filter (fn [[k v]] (= full-path (take path-length k))) (:puts tx))))
     coll)
   full-path))

(defn collect
  "Returns a map assembled from the keys beginning with the
  that begin with the complete supplied `path` conjoined with
  the transaction prefix.

  Ex. If the transaction `tx` has no prefix & the database contains:

  [ key ]                   | val
  -------------------------------
  [:dept :art      :alice]  |  1
  [:dept :bio-tech   :bob]  |  2
  [:dept :biology  :chloe]  |  3
  [:dept :biology :daniel]  |  4
  [:dept :biology  :ellen]  |  5
  [:dept :botany   :frank]  |  6
  -------------------------------

  (collect tx [] -> {:dept
                     {:art {:alice 1}
                      :bio-tech {:bob 2}
                      :biology {:chloe 3
                                :daniel 4
                                :ellen 5}
                      :botant  {:frank 6}}}


  (collect tx [:dept :biology]) -> {:chloe 3
                                    :daniel 4
                                    :ellen 5}}


  Ex. If, however the transaction prefix was `[:dept]`:

  (seek tx [:biology]) -> {:chloe 3
                           :daniel 4
                           :ellen 5}}"
  [tx full-path]
  (complete-collection tx full-path (reduce reduce-assoc {} (seek tx full-path))))


(defn- del-path [tx path]
  (reduce (fn [tx raw-key] (store/b+remove tx raw-key))
          tx
          (seek tx path path :keys-only true :no-decode true)))

(defn- assoc-helper [tx path x]
  (if (map? x)
    (reduce (fn [tx [k v]] (assoc-helper tx (conj path k) v)) tx x)
    (put-val tx path x)))

(defn assoc-map [tx path v]
  (-> tx
      (del-path path)
      (assoc-helper path v)))

(defn- collect-delete [tx path]
  (let [values (seek tx path path :no-decode true)]
    {:original (complete-collection tx path
                                    (reduce reduce-assoc {} (map #(update % 0 pathwise/decode) values)))
     :tx (reduce (fn [t [raw-key _]] (store/b+remove tx raw-key)) tx values)}))

(defn update-map [tx path f & args]
  (let [{:keys [tx original]} (collect-delete tx path)
        updated (apply f original args)]
    (assoc-helper tx path updated)))

(defn delete-map [tx path]
  (del-path tx path))

(defn put-map [tx path x]
  (cond
    (map? x) (reduce (fn [tx [k v]] (put-map tx (conj path k) v)) tx x)
    (nil? x) (del-path tx path)
    :else (-> tx
              (del-path path)
              (put-val path x))))

(defn put-map-update [tx path f & args]
  (let [original (collect tx path)
        updated (apply f original args)]
    (put-map tx path updated)))

(defn search [tx path & {:keys [to-suffix limit reverse partial]}])
