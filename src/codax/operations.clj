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
  (let [k (pathwise/encode path)
        old-val (store/b+get tx k)
        new-val (apply f old-val args)]
    (store/b+insert tx k new-val)))

(defn seek
  ([tx] (seek tx nil nil))
  ([tx path] (seek tx path path))
  ([tx start-path end-path & {:keys [limit no-decode only-keys only-vals partial]}]
   (let [order-char (if partial nil (char 0x00))
         start (if (empty? start-path)
                 (str (char 0x00))
                 (str (pathwise/partially-encode start-path) order-char))
         end (if (empty? end-path)
               (str (char 0xff))
               (str (pathwise/partially-encode end-path) order-char (char 0xff)))
         results (store/b+seek tx start end :limit limit)]
     (cond
       (and no-decode only-keys) (map first results)
       no-decode results
       only-keys (map #(pathwise/decode (first %)) results)
       only-vals (map second results)
       :else (map #(update % 0 pathwise/decode) results)))))

(defn- reduce-assoc [coll [k v]]
  (if (not (vector? k))
    coll
    (assoc-in coll k v)))

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
  [tx path]
  (get-in (reduce reduce-assoc {} (seek tx path)) path))

(defn- del-path [tx path]
  (reduce (fn [tx raw-key] (store/b+remove tx raw-key))
          tx
          (seek tx path path :only-keys true :no-decode true)))

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
    {:original (get-in (reduce reduce-assoc {} (map #(update % 0 pathwise/decode) values)) path)
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
