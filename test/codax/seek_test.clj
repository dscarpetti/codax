(ns codax.seek-test
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.string :as str]
   [clojure.test :as t :refer :all]
   [codax.core :refer :all]))

(def ^:dynamic *logging* false)

(def ansi-color-table
  {:reset "\u001B[0m"
   :black "\u001B[30m"
   :red "\u001B[31m"
   :green "\u001B[32m"
   :yellow "\u001B[33m"
   :blue "\u001B[34m"
   :purple "\u001B[35m"
   :cyan "\u001B[36m"
   :white "\u001B[37m"
   :bg-black "\u001B[40m"
   :bg-red "\u001B[41m"
   :bg-green "\u001B[42m"
   :bg-yellow "\u001B[43m"
   :bg-blue "\u001B[44m"
   :bg-purple "\u001B[45m"
   :bg-cyan "\u001B[46m"
   :bg-white "\u001B[47m"})


(defn color [[& colors] & ss]
  (str
   (apply str (map #(ansi-color-table %) colors))
   (str/join " " ss)
   (ansi-color-table :reset)))



(defn left-pad [n x]
  (let [s (str x)
        p (if (< (count s) n)
            (apply str (repeat (- n (count s)) " "))
            "")]
    (str p s)))

(defmacro tell-time [[colors & s] & body]
  `(let [_# (when *logging* (print (color ~colors ~@s " ")))
         _# (flush)
         time# (System/nanoTime)
         res# (do ~@body)]
     (when *logging* (println (color ~colors (left-pad 8 (format "%.1f ms" (/ (- (System/nanoTime) time#) 1000000.0))))))
     res#))

(defn logln [colors & ss]
  (let [cs (apply str (map ansi-color-table colors))
        data (conj (vec ss) (:reset ansi-color-table))]
    (when *logging* (apply println cs data))))


;;;;

(defn finalize-sim-op [data limit rev]
  (let [data (if rev (reverse data) data)]
    (if limit (take limit data) data)))


(defn sim-seek-at [dataset & {:keys [limit reverse]}]
  (finalize-sim-op dataset limit reverse))

(defn sim-seek-prefix [dataset val-prefix & {:keys [limit reverse]}]
  (finalize-sim-op (filter #(re-matches (re-pattern (str "^" val-prefix ".*")) (first %)) dataset)
                   limit reverse))

(defn sim-seek-from [dataset start-val & {:keys [limit reverse]}]
  (finalize-sim-op (filter #(>= 0 (compare start-val (first %))) dataset)
                   limit reverse))

(defn sim-seek-to [dataset end-val & {:keys [limit reverse]}]
  (finalize-sim-op (filter #(<= 0 (compare end-val (first %))) dataset)
                   limit reverse))

(defn sim-seek-range [dataset start-val end-val & {:keys [limit reverse]}]
  (finalize-sim-op (filter #(and
                             (>= 0 (compare start-val (first %)))
                             (<= 0 (compare end-val (first %))))
                           dataset)
                   limit reverse))

;;;;


(defn reopen-db! [db]
  (let [path (:path @db)]
    (close-database! @db)
    (reset! db (open-database! path))))

(def ^:dynamic *logged-result-length* 20)

(defmacro def-seek-test [name [prefix-path dataset-data] & body]
  `(deftest ~name
     (destroy-database! "test-databases/seek")
     (let [~'prefix-path ~prefix-path
           ~'dataset ~dataset-data
           ~'db (atom (open-database! "test-databases/seek"))]
       (tell-time
        [[:purple] "populating db with" (count ~'dataset) "records"]
        (with-write-transaction [@~'db tx#]
          (reduce (fn [tx# [k# v#]]
                    (assoc-at tx# (conj ~'prefix-path k#) v#))
                  tx# ~'dataset)))
       ~@body)))

(defmacro test-seek-at [limit reverse]
  `(let [control-result# (sim-seek-at ~'dataset :limit ~limit :reverse ~reverse)
         test-result# (seek-at! @~'db ~'prefix-path :limit ~limit :reverse ~reverse)]
     (logln [:purple] "test-seek-at" "limit:" ~limit "reverse:" ~reverse)
     (logln [:blue] "rslt:" (count test-result#) (map first (take *logged-result-length* test-result#)))
     (logln [:yellow] "ctrl:" (count control-result#) (map first (take *logged-result-length* control-result#)))
     (is (= test-result# control-result#) ["test-seek-at" "limit:" ~limit "reverse:" ~reverse])))

(defmacro test-seek-at* [[& limits]]
  `(do ~@(reduce (fn [ls# limit#] (conj ls#
                                        `(test-seek-at ~limit# false)
                                        `(test-seek-at ~limit# true)))
               [] limits)))

(defmacro test-seek-prefix [val-prefix limit reverse]
  `(let [control-result# (sim-seek-prefix ~'dataset ~val-prefix :limit ~limit :reverse ~reverse)
         test-result# (seek-prefix! @~'db ~'prefix-path ~val-prefix :limit ~limit :reverse ~reverse)]
     (logln [:purple] "test-seek-prefix" "val-prefix:" ~val-prefix "limit:" ~limit "reverse:" ~reverse)
     (logln [:blue] "rslt:" (count test-result#) (map first (take *logged-result-length* test-result#)))
     (logln [:yellow] "ctrl:" (count control-result#) (map first (take *logged-result-length* control-result#)))
     (is (= test-result# control-result#) ["test-seek-prefix" "val-prefix:" ~val-prefix "limit:" ~limit "reverse:" ~reverse])))

(defmacro test-seek-prefix* [val-prefix limits]
  `(do ~@(reduce (fn [ls# limit#] (conj ls#
                                        `(test-seek-prefix ~val-prefix ~limit# false)
                                        `(test-seek-prefix ~val-prefix ~limit# true)))
               [] limits)))


(defmacro test-seek-from [start-val limit reverse]
  `(let [control-result# (sim-seek-from ~'dataset ~start-val :limit ~limit :reverse ~reverse)
         test-result# (seek-from! @~'db ~'prefix-path ~start-val :limit ~limit :reverse ~reverse)]
     (logln [:purple] "test-seek-from" "start-val:" ~start-val "limit:" ~limit "reverse:" ~reverse)
     (logln [:blue] "rslt:" (count test-result#) (map first (take *logged-result-length* test-result#)))
     (logln [:yellow] "ctrl:" (count control-result#) (map first (take *logged-result-length* control-result#)))
     (is (= test-result# control-result#) ["test-seek-from" "start-val:" ~start-val "limit:" ~limit "reverse:" ~reverse])))

(defmacro test-seek-from* [start-val limits]
  `(do ~@(reduce (fn [ls# limit#] (conj ls#
                                        `(test-seek-from ~start-val ~limit# false)
                                        `(test-seek-from ~start-val ~limit# true)))
                 [] limits)))

(defmacro test-seek-to [end-val limit reverse]
  `(let [control-result# (sim-seek-to ~'dataset ~end-val :limit ~limit :reverse ~reverse)
         test-result# (seek-to! @~'db ~'prefix-path ~end-val :limit ~limit :reverse ~reverse)]
     (logln [:purple] "test-seek-to" "end-val:" ~end-val "limit:" ~limit "reverse:" ~reverse)
     (logln [:blue] "rslt:" (count test-result#) (map first (take *logged-result-length* test-result#)))
     (logln [:yellow] "ctrl:" (count control-result#) (map first (take *logged-result-length* control-result#)))
     (is (= test-result# control-result#) ["test-seek-to" "end-val:" ~end-val "limit:" ~limit "reverse:" ~reverse])))

(defmacro test-seek-to* [start-val limits]
  `(do ~@(reduce (fn [ls# limit#] (conj ls#
                                        `(test-seek-to ~start-val ~limit# false)
                                        `(test-seek-to ~start-val ~limit# true)))
                 [] limits)))

(defmacro test-seek-range [start-val end-val limit reverse]
  `(let [control-result# (sim-seek-range ~'dataset ~start-val ~end-val :limit ~limit :reverse ~reverse)
         test-result# (seek-range! @~'db ~'prefix-path ~start-val ~end-val :limit ~limit :reverse ~reverse)]
    (logln [:purple] "test-seek-range" "start-val:" ~start-val "end-val:" ~end-val "limit:" ~limit "reverse:" ~reverse)
    (logln [:blue] "rslt:" (count test-result#) (map first (take *logged-result-length* test-result#)))
    (logln [:yellow] "ctrl:" (count control-result#) (map first (take *logged-result-length* control-result#)))
    (is (= test-result# control-result#) ["test-seek-range" "start-val:" ~start-val "end-val:" ~end-val "limit:" ~limit "reverse:" ~reverse])))

(defmacro test-seek-range* [start-val end-val limits]
  `(do ~@(reduce (fn [ls# limit#] (conj ls#
                                        `(test-seek-range ~start-val ~end-val ~limit# false)
                                        `(test-seek-range ~start-val ~end-val ~limit# true)))
                 [] limits)))

;;;;;;;

(defn create-string-dataset [key-length child-count]
  (let [child-key-paths [[:a] [:b :c] [:d] [:e :f :g] [:h] [:i :j :k :l] [:m] [:n :o :p :q :r :s]]
        simple (not child-count)
        child-count (if child-count (min child-count (count child-key-paths)) 1)
        letters (take (inc key-length) ["" "a" "b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n" "o" "p" "q" "r" "s" "t" "u" "v" "w" "x" "y" "z"])
        keyset (sort (set (for [x (rest letters) y letters z letters xx letters]
                            (str x y z xx))))
        ckvs (if simple (range) (map #(vector %1 %2) (cycle child-key-paths) (range)))]
    (loop [pairs []
           [k & ks] keyset
           ckvs ckvs]
      (if (nil? k)
        pairs
        (let [v (if simple (first ckvs) (reduce (fn [m [k v]] (assoc-in m k v)) {} (take child-count ckvs)))]
          (recur (conj pairs [k v]) ks (drop child-count ckvs)))))))

(def string-dataset-tiny (create-string-dataset 2 false))
(def string-dataset-small (create-string-dataset 2 8))
(def string-dataset-medium (create-string-dataset 5 8))
(def string-dataset-large (create-string-dataset 8 8))

(def-seek-test tiny-alpha-test [[:alphabet] string-dataset-tiny]
  (test-seek-at* [nil 10 100])
  (test-seek-prefix* "a" [nil 10 100])
  (test-seek-prefix* "b" [nil 10 100])
  (test-seek-prefix* "c" [nil 10 100])
  (test-seek-prefix* "d" [nil 10 100])
  (test-seek-prefix* "e" [nil 10 100])
  (test-seek-prefix* "f" [nil 10 100])
  (test-seek-prefix* "g" [nil 10 100])
  (test-seek-prefix* "A" [nil 10 100])
  (test-seek-prefix* "" [nil 10 100])

  (test-seek-from* "A" [nil 10 100])
  (test-seek-from* "0" [nil 10 100])
  (test-seek-from* "b" [nil 10 100])
  (test-seek-from* "z" [nil 10 100])
  (test-seek-from* "b" [nil 10 100])
  (test-seek-from* "ab" [nil 10 100])
  (test-seek-from* "cc" [nil 10 100])

  (test-seek-to* "A" [nil 10 100])
  (test-seek-to* "0" [nil 10 100])
  (test-seek-to* "b" [nil 10 100])
  (test-seek-to* "z" [nil 10 100])
  (test-seek-to* "b" [nil 10 100])
  (test-seek-to* "ab" [nil 10 100])
  (test-seek-to* "cc" [nil 10 100])

  (test-seek-range* "a" "c" [nil 10 100])
  (test-seek-range* "b" "c" [nil 10 100])
  (test-seek-range* "a" "b" [nil 10 100])
  (test-seek-range* "e" "a" [nil 10 100])
  (test-seek-range* "b" "zz" [nil 10 100])
  (test-seek-range* "0" "b" [nil 10 100])
  (test-seek-range* "A" "B" [nil 10 100])
  (test-seek-range* "X" "Z" [nil 10 100])
  (test-seek-range* "x" "z" [nil 10 100]))


(def-seek-test small-alpha-test [[:alphabet] string-dataset-small]
  (test-seek-at* [nil 10 100])
  (test-seek-prefix* "a" [nil 10 100])
  (test-seek-prefix* "b" [nil 10 100])
  (test-seek-prefix* "c" [nil 10 100])
  (test-seek-prefix* "d" [nil 10 100])
  (test-seek-prefix* "e" [nil 10 100])
  (test-seek-prefix* "f" [nil 10 100])
  (test-seek-prefix* "g" [nil 10 100])
  (test-seek-prefix* "A" [nil 10 100])
  (test-seek-prefix* "" [nil 10 100])


  (test-seek-from* "A" [nil 10 100])
  (test-seek-from* "0" [nil 10 100])
  (test-seek-from* "b" [nil 10 100])
  (test-seek-from* "z" [nil 10 100])
  (test-seek-from* "b" [nil 10 100])
  (test-seek-from* "ab" [nil 10 100])
  (test-seek-from* "cc" [nil 10 100])

  (test-seek-to* "A" [nil 10 100])
  (test-seek-to* "0" [nil 10 100])
  (test-seek-to* "b" [nil 10 100])
  (test-seek-to* "z" [nil 10 100])
  (test-seek-to* "b" [nil 10 100])
  (test-seek-to* "ab" [nil 10 100])
  (test-seek-to* "cc" [nil 10 100])


  (test-seek-range* "a" "c" [nil 10 100])
  (test-seek-range* "b" "c" [nil 10 100])
  (test-seek-range* "a" "b" [nil 10 100])
  (test-seek-range* "e" "a" [nil 10 100])
  (test-seek-range* "b" "zz" [nil 10 100])
  (test-seek-range* "0" "b" [nil 10 100])
  (test-seek-range* "A" "B" [nil 10 100])
  (test-seek-range* "X" "Z" [nil 10 100])
  (test-seek-range* "x" "z" [nil 10 100])

  (test-seek-range* "e" "a" [nil 10 100]))

(def-seek-test medium-alpha-test-1 [["alphabet"] string-dataset-medium]
  (test-seek-at* [nil 10 100 1000])

  (test-seek-prefix* "a" [nil 10 100 1000])
  (test-seek-prefix* "b" [nil 10 100 1000])
  (test-seek-prefix* "c" [nil 10 100 1000])
  (test-seek-prefix* "d" [nil 10 100 1000])
  (test-seek-prefix* "e" [nil 10 100 1000])
  (test-seek-prefix* "f" [nil 10 100 1000])
  (test-seek-prefix* "g" [nil 10 100 1000])
  (test-seek-prefix* "A" [nil 10 100 1000])
  (test-seek-prefix* "" [nil 10 100 1000])


  (test-seek-from* "A" [nil 10 100 1000])
  (test-seek-from* "0" [nil 10 100 1000])
  (test-seek-from* "b" [nil 10 100 1000])
  (test-seek-from* "z" [nil 10 100 1000])
  (test-seek-from* "b" [nil 10 100 1000])
  (test-seek-from* "ab" [nil 10 100 1000])
  (test-seek-from* "cc" [nil 10 100 1000])

  (test-seek-to* "A" [nil 10 100 1000])
  (test-seek-to* "0" [nil 10 100 1000])
  (test-seek-to* "b" [nil 10 100 1000])
  (test-seek-to* "z" [nil 10 100 1000])
  (test-seek-to* "b" [nil 10 100 1000])
  (test-seek-to* "ab" [nil 10 100 1000])
  (test-seek-to* "cc" [nil 10 100 1000]))

(def-seek-test medium-alpha-test-2 [["alphabet"] string-dataset-medium]
  (test-seek-range* "a" "c" [nil 10 100 1000])
  (test-seek-range* "b" "c" [nil 10 100 1000])
  (test-seek-range* "a" "b" [nil 10 100 1000])
  (test-seek-range* "e" "a" [nil 10 100 1000])
  (test-seek-range* "b" "zz" [nil 10 100 1000])
  (test-seek-range* "0" "b" [nil 10 100 1000])
  (test-seek-range* "A" "B" [nil 10 100 1000])
  (test-seek-range* "X" "Z" [nil 10 100 1000])
  (test-seek-range* "x" "z" [nil 10 100 1000])
  (test-seek-range* "e" "a" [nil 10 100 1000])

  (test-seek-range* "ab" "cd" [nil 10 100 1000])
  (test-seek-range* "b" "c" [nil 10 100 1000])
  (test-seek-range* "daa" "daac" [nil 10 100 1000]))


(comment
  (def-seek-test large-alpha-test [[26] string-dataset-large]
    (test-seek-at* [nil 10 100 1000 10000])
    (test-seek-prefix* "a" [nil 10 100 1000 10000])
    (test-seek-prefix* "b" [nil 10 100 1000 10000])
    (test-seek-prefix* "c" [nil 10 100 1000 10000])
    (test-seek-from* "b" [nil 10 100 1000 10000])
    (test-seek-from* "e" [nil 10 100 1000 10000])
    (test-seek-to* "ab" [nil 10 100 1000 10000])
    (test-seek-to* "ea" [nil 10 100 1000 10000])
    (test-seek-range* "a" "z" [nil 10 100 1000 10000])
    (test-seek-range* "aaaaa" "ccddd" [nil 10 100 1000 10000])
    (test-seek-range* "ab" "cd" [nil 10 100 1000 10000])
    (test-seek-range* "d" "c" [nil 10 100 1000 10000])
    (test-seek-range* "b" "c" [nil 10 100 1000 10000])
    (test-seek-range* "daa" "daac" [nil 10 100 1000])
    (test-seek-range* "e" "a" [nil 10 100 10000])))

;;;;;;;

(defn create-numeric-dataset [n]
  (take (* 2 n) (map #(vector %1 (str "num: " %2)) (range (- n) n) (range (- n) n))))



(defmacro num-seek-from [max]
  (let [limits (concat (cons nil (take-while #(> % 1) (iterate #(int (/ % 5)) (* 3 max)))) [1])]
    `(doseq [n# (range (- ~max) ~max)]
       (when (zero? (mod n# 100)) (logln [:green] "from" n#))
       (test-seek-from* n# ~limits))))

(defmacro num-seek-to [max]
  (let [limits (concat (cons nil (take-while #(> % 1) (iterate #(int (/ % 5)) (* 3 max)))) [1])]
    `(doseq [n# (range (- ~max) ~max)]
       (when (zero? (mod n# 100)) (logln [:green] "to" n#))
       (test-seek-to* n# ~limits))))

(defmacro num-seek-range [max]
  (let [limits (concat (cons nil (take-while #(> % 1) (iterate #(int (/ % 5)) (* 3 max)))) [1])]
    `(doseq [n1# (range (- ~max) ~max)
             n2# (range (- ~max) ~max)]
       (when (zero? (+ (mod n1# 100) (mod n2# 100))) (logln [:green] "range" n1# n2#))
       (test-seek-range* n1# n2# ~limits))))


(def-seek-test exhaustive-one-level [[:numbers] (create-numeric-dataset 45)]
  (test-seek-at* [nil 10 100 500])
  (num-seek-from 90)
  (num-seek-to 90)
  (num-seek-range 90))


(comment

  (println "exhaustive numeric test starting: will take hours to run")

  (def-seek-test exhaustive-two-level [[:numbers] (create-numeric-dataset 450)]
    (test-seek-at* [nil 10 100 500])
    (num-seek-from 500)
    (num-seek-to 500)
    (num-seek-range 500))
  )
