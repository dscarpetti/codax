(ns codax.pathwise
  (:require
   [clojure.string :as string]
   [clj-time.format :as joda-time-format]))

(def +delim+ (char 0x00))
(def +vector+ (char 0xa0))

(declare encode-element decode-element)

(defn encode-vector [v]
  (apply str (map encode-element v)))

(defn decode-vector [cs]
  (if (empty? cs)
    []
  (loop [prev-c +delim+
         cs cs
         active []
         acc []
         depth 0]
    (let [c (first cs)
          cs (rest cs)
          new-active (conj active c)]
      (cond
        (nil? c) (mapv decode-element (conj acc active))
        (and (zero? depth) (not (empty? active))) (recur prev-c (cons c cs) [] (conj acc active) 0)
        (= c +delim+) (recur c cs new-active acc (dec depth))
        (= prev-c +delim+) (recur c cs new-active acc (inc depth))
        (= prev-c +vector+) (recur c cs new-active acc (inc depth))
        :else (recur c cs new-active acc depth))))))

(defn encode-number [n]
  (let [offset (if (<= 0 n) 0 -1000)
        prefix (if (<= 0 n) "_" "-")
        n-str (if (integer? n) (str n) (format "%f" n))
        n-str (if (<= 0 n) n-str (reduce str (map #(if (= \. %) "." (str (- 9 (read-string (str %))))) (subs n-str 1))))
        len (.length (re-find #"[^.]*" n-str))]
    (str prefix (re-find #"\d\d\d$" (str "0" "0" (+ offset len))) "x" n-str)))

(defn decode-number [n]
  (read-string
   (if (= \- (first n))
     (reduce str "-" (map (fn [x] (if (= \. x) x (- 9 (read-string (str x)))) ) (nthrest n 5)))
     (reduce str "" (nthrest n 5)))))

(defn pos-infinity? [x]
  (and (number? x) (Double/isInfinite x) (pos? x)))

(defn neg-infinity? [x]
  (and (number? x) (Double/isInfinite x) (neg? x)))

(defn encode-symbol [s]
  (str s))

(defn decode-symbol [s]
  (symbol (string/join s)))

(defn encode-keyword [k]
  (string/join (rest (str k))))

(defn decode-keyword [s]
  (keyword (string/join s)))

(defn encode-string [s] s)
(defn decode-string [s] (string/join s))

(def joda-time-formatter (joda-time-format/formatters :basic-date-time))

(defn joda-time? [x]
  (instance? org.joda.time.DateTime x))

(defn encode-joda-time [d]
  (joda-time-format/unparse joda-time-formatter d))

(defn decode-joda-time [d]
  (joda-time-format/parse joda-time-formatter (string/join d)))

(defn java-time? [x]
  (instance? java.time.Instant x))

(defn encode-java-time [t]
  (str t))

(defn decode-java-time [t]
  (java.time.Instant/parse (string/join t)))

(defmacro build-encoding-functions [type-specs]
  (let [el (gensym "el")
        type-char (gensym "type-char")
        body (gensym "body")]
    `(do
       (defn ~'encode-element [~el]
         (cond
           ~@(reduce concat
                     (vec
                      (concat (map (fn [[nick {:keys [predicate hex encoder]}]]
                                     [`(~predicate ~el)
                                      `(str (char ~hex) (~encoder ~el) +delim+)])
                                   type-specs))))
           :else (throw (Exception. (str "no method for encoding `" ~el "`")))))
       (defn ~'decode-element [~el]
         (let [~type-char (first ~el)
               ~body (butlast (rest ~el))]
           (cond
             ~@(reduce concat
                       (vec
                        (concat (map (fn [[nick {:keys [hex decoder]}]]
                                       (let [c (char hex)]
                                         [`(= ~type-char ~c)
                                          `(~decoder ~body)]))
                                     type-specs))))
             :else (throw (Exception. "unrecognized type")))))
       (def encode encode-element)
       (def decode decode-element))))

(build-encoding-functions
 {:nil {:predicate nil?
        :hex 0x10
        :encoder (fn [_] "")
        :decoder (fn [_] nil)}
  :symbol {:predicate symbol?
           :hex 0x68
           :encoder encode-symbol
           :decoder decode-symbol}
  :false {:predicate false?
          :hex 0x20
          :encoder (fn [_] "")
          :decoder (fn [_] false)}
  :true {:predicate true?
         :hex 0x21
         :encoder (fn [_] "")
         :decoder (fn [_] true)}
  :joda-time {:predicate joda-time?
              :hex 0x24
              :encoder encode-joda-time
              :decoder decode-joda-time}
  :java-time {:predicate java-time?
              :hex 0x25
              :encoder encode-java-time
              :decoder decode-java-time}
  :neg-infinity {:predicate neg-infinity?
                 :hex 0x30
                 :encoder (fn [_] "")
                 :decoder (fn [_] Double/NEGATIVE_INFINITY)}
  :number {:predicate number?
           :hex 0x31
           :encoder encode-number
           :decoder decode-number}
  :pos-infinity {:predicate pos-infinity?
                 :hex 0x32
                 :encoder (fn [_] "")
                 :decoder (fn [_] Double/POSITIVE_INFINITY)}
  :keyword {:predicate keyword?
            :hex 0x69
            :encoder encode-keyword
            :decoder decode-keyword}
  :string {:predicate string?
           :hex 0x70
           :encoder encode-string
           :decoder decode-string}
  :vector {:predicate #(or (vector? %) (list? %))
           :hex 0xa0
           :encoder encode-vector
           :decoder decode-vector}})


(def +partial-encoding-truncation-pattern+ (re-pattern (str +delim+ "+$")))
(defn partially-encode [x]
  (string/replace (encode-element x) +partial-encoding-truncation-pattern+ ""))
