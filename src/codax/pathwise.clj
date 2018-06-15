(ns codax.pathwise
  (:require
   [clojure.string :as str]
   [clj-time.format :as joda]))

(def +delim+ (char 0x00))
(def +vector+ (char 0xa0))

(defprotocol PathwiseEncoding "encode elements" (encode [x]))

(defmulti decode first)

(defn partially-encode [x]
  (str/replace (encode x) (re-pattern (str +delim+ "+$")) ""))

(defmacro defpathtype [[hex-code & types] encoder decoder]
  (let [ch (char hex-code)]
    `(do
       ~@(map (fn [type]
                `(extend-type ~type
                   PathwiseEncoding
                   (encode [el#]
                     (str ~ch (~encoder el#) +delim+))))
              types)
       (defmethod decode ~ch
         [el#]
         (let [body# (subs el# 1 (dec (count el#)))]
           (~decoder body#))))))

(defpathtype [0x10 nil] (fn [_] "") (fn [_] nil))

(defpathtype [0x68 clojure.lang.Symbol] str symbol)

(defpathtype [0x69 clojure.lang.Keyword] #(subs (str %) 1) keyword)

(defpathtype [0x70 java.lang.String] identity identity)

(defpathtype [0x25 java.time.Instant] str java.time.Instant/parse)

(defpathtype [0x24 org.joda.time.DateTime]
  (partial joda/unparse (joda/formatters :basic-date-time))
  (partial joda/parse (joda/formatters :basic-date-time)))

(defpathtype [0xa0 clojure.lang.PersistentVector clojure.lang.PersistentList]
  #(apply str (map encode %))
  #(if (empty? %)
     []
     (let [tokens (str/split % (re-pattern (str "(?<=" +delim+ ")|(?=" +vector+ ")")))
           result (loop [[t & ts] tokens
                         result []]
                    (cond
                      (nil? t) result
                      (= t (str +vector+ +delim+)) (recur ts (conj result t))
                      (str/starts-with? t (str +vector+))
                      (let [[sub-result remainder] (loop [[s & ss] ts
                                                          sub-result t
                                                          depth 1]
                                                     (if (zero? depth)
                                                       [sub-result (cons s ss)]
                                                       (let [depth (cond
                                                                     (= s (str +vector+ +delim+)) depth
                                                                     (str/starts-with? s (str +vector+)) (inc depth)
                                                                     (= s (str +delim+)) (dec depth)
                                                                     :else depth)]
                                                         (recur ss (str sub-result s) (int depth)))))]

                        (recur remainder (conj result sub-result)))
                      :else (recur ts (conj result t))))]
       (mapv decode result))))

(defn te [form]
  (let [encoded (encode form)
        decoded (decode encoded)]
    (println form)
    (println decoded)
    (= form decoded)))

;;(te [[[[[[]]]:a]]])

;; support for legacy encodings of numbers and booleans
(defn- encode-number [n]
  (let [n (if (ratio? n) (double n) n)
        offset (if (<= 0 n) 0 -1000)
        prefix (if (<= 0 n) "_" "-")
        n-str (if (integer? n) (str n) (format "%f" n))
        n-str (if (<= 0 n) n-str (reduce str (map #(if (= \. %) "." (str (- 9 (read-string (str %))))) (subs n-str 1))))
        len (count (re-find #"[^.]*" n-str))]
    (str prefix (re-find #"\d\d\d$" (str "0" "0" (+ offset len))) "x" n-str)))

(defn decode-number [n]
  (read-string
   (if (= \- (first n))
     (reduce str "-" (map (fn [x] (if (= \. x) x (- 9 (read-string (str x)))) ) (nthrest n 5)))
     (reduce str "" (nthrest n 5)))))

(defn- manual-encode
  ([hex-code] (str (char hex-code) +delim+))
  ([hex-code body] (str (char hex-code) body +delim+)))

(extend-type java.lang.Object
  PathwiseEncoding
  (encode [x]
    (cond
      (false? x) (manual-encode 0x20)
      (true? x) (manual-encode 0x21)
      (and (number? x) (Double/isInfinite x) (neg? x)) (manual-encode 0x30)
      (and (number? x) (Double/isInfinite x) (pos? x)) (manual-encode 0x32)
      (number? x) (manual-encode 0x31 (encode-number x)))))

(defmethod decode (char 0x20) [_] false)
(defmethod decode (char 0x21) [_] true)
(defmethod decode (char 0x30) [_] Double/NEGATIVE_INFINITY)
(defmethod decode (char 0x31) [el] (decode-number (subs el 1 (dec (count el)))))
(defmethod decode (char 0x32) [_] Double/POSITIVE_INFINITY)

(comment
  "This is a possible future upgrade path which would break backward-compatibility for databases using:"
  "`true` `false` `NEGATIVE_INFINITY` or `POSITIVE_INFINITY` in paths."

  (defpathtype [0x20 java.lang.Boolean]
    #(if % "t" "f")
    #(= % "t"))

  (defpathtype [0x31 java.lang.Byte java.lang.Short java.lang.Integer java.lang.Long java.lang.Float java.lang.Double]
    #(cond
       (and (Double/isInfinite %) (neg? %)) "!"
       (Double/isInfinite %) "i"
       :else (let [offset (if (<= 0 %) 0 -1000)
                   prefix (if (<= 0 %) "_" "-")
                   n-str (if (integer? %) (str %) (format "%f" %))
                   n-str (if (<= 0 %) n-str (reduce str (map (fn [x] (if (= \. x) "." (str (- 9 (read-string (str x)))))) (subs n-str 1))))
                   len (count (re-find #"[^.]*" n-str))]
               (str prefix (re-find #"\d\d\d$" (str "0" "0" (+ offset len))) "x" n-str)))
    #(cond
       (= % "!") Double/NEGATIVE_INFINITY
       (= % "i") Double/POSITIVE_INFINITY
       :else (read-string
              (if (= \- (first %))
                (reduce str "-" (map (fn [x] (if (= \. x) x (- 9 (read-string (str x)))) ) (nthrest % 5)))
                (reduce str "" (nthrest % 5))))))

;;; compatibility decoders for old encodings
  (defmethod decode (char 0x21) [_] true)
  (defmethod decode (char 0x30) [_] Double/NEGATIVE_INFINITY)
  (defmethod decode (char 0x32) [_] Double/POSITIVE_INFINITY)
  )
