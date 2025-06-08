(ns codax.pathwise-test
  (:require
   [codax.test-logging :refer [logln]]
   [codax.pathwise :refer :all]
   [codax.pathwise-legacy :as legacy]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [clojure.pprint :refer [pprint]]))

;;;; test encoding/decoding against new and legacy pathwise versions

(defmacro defet [name form]
  `(deftest ~name
     (let [form# ~form
           new-encoded# (encode form#)
           old-encoded# (legacy/encode form#)
           new->new# (decode new-encoded#)
           new->old# (legacy/decode new-encoded#)
           old->new# (decode old-encoded#)
           old->old# (legacy/decode old-encoded#)]
       (is (= new-encoded# old-encoded#) (str "new and old encodings do not match\n" "old: " old-encoded# "\nnew: " new-encoded#))
       (is (= form# new->new#) (str "new->new failed\n" "before: " form# "\nafter: " new->new#))
       (is (= form# new->old#) (str "new->old failed\n" "before: " form# "\nafter: " new->old#))
       (is (= form# old->new#) (str "old->new failed\n" "before: " form# "\nafter: " old->new#))
       (is (= form# old->old#) (str "old->old failed\n" "before: " form# "\nafter: " old->old#))
       (is (= form# new->new# new->old# old->new# old->old#) "unspecified inequality in conversion between versions"))))

(defet negative-infinity Double/NEGATIVE_INFINITY)
(defet positive-infinity Double/POSITIVE_INFINITY)

(defet zero 0)
(defet zero-float (float 0.0))
(defet negative-0 -0)
(defet negative-0-float (float -0.0))

(defet byte-val (byte 127))
(defet negative-byte-val (byte -127))

(defet short-val (short 200))
(defet negative-short-val (short -127))

(defet negative-int-1 (int -12345678))
(defet positive-int-1 (int 12345678))
(defet negative-int-2 (int -98765432))
(defet positive-int-2 (int 98765432))

(defet negative-long-1 (long -1234567890))
(defet positive-long-1 (long 1234567890))
(defet negative-long-2 (long -9876543210))
(defet positive-long-2 (long 9876543210))

(defet negative-float-1 (float -99999999.123))
(defet positive-float-1 (float 99999999.123))

(defet negative-double-1 (double -1000.999))
(defet positive-double-1 (double 1000.999))
(defet negative-double-2 (double -99999999.123))
(defet positive-double-2 (double 99999999.123))

(defet nil-val nil)
(defet true-val true)
(defet false-val false)

(defet symbol-val 'a-symbol)
(defet symbol-val 'ANOTHER-SYMBOL)
(defet namespaced-symbol-val 'some-ns/a-symbol)

(defet keyword-val :keyword)
(defet namespaced-keyword-val :some-ns/hi)
(defet current-namespaced-keyword-val ::keyword)

(defet string-val "string")

(defet long-string-val "one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty")

(defet java-time (java.time.Instant/now))

(defet vec-1 [0 0.0 -1 1 -1.1 1.1 nil true false 'symbol "string" :keyword (java.time.Instant/now)])
(defet vec-2 [nil])
(defet vec-3 [nil nil])
(defet vec-4 [nil nil nil])
(defet vec-5 [[nil] nil])
(defet vec-6 [nil [[nil]]])
(defet vec-7 [nil [] nil])
(defet vec-8 [[] :a []])
(defet vec-9 [[] [] []])

(defet empty-vec [])
(defet empty-in-empty [[]])
(defet empty-in-empty-in-empty [[[]]])
(defet empty-in-empty-in-empty-in-empty [[[[]]]])

(defet pathological-1 [[[[[[[[[[[[]]]]]]]]]]]])
(defet pathological-2 [[[[[[[:a[[[[[]]]]]]]]]]]])
(defet pathological-3 [[[[[[[[:a][[[[[]]]]]]]]]]]])
(defet pathological-4 [[[[[[[[[:a]][[[[[]]]]]]]]]]]])
(defet pathological-5 [[[[[[[:a[[[[[:b]]]]]]]]]]]])
(defet pathological-6 [[[[[[[[:a][[[[[:b]]]]]]]]]]]])
(defet pathological-7 [[[[[[[[[:a]][[[[[:b]]]]]]]]]]]])
(defet pathological-8 [[[[[[[[[[:a]]][[[[[:b]]]]]]]]]]]])
(defet pathological-9 [[[[[[[:a[[[[[:b]]]]]]]]]]]:c])
(defet pathological-10 [[[[[[[[:a][[[[[:b]]]]]]]]]]]:c])
(defet pathological-11 [[[[[[[[[:a]][[[[[:b]]]]]]]]]]]:c])
(defet pathological-12 [[[[[[[[[[:a]]][[[[[:b]]]]]]]]]]]:c])
(defet pathological-13 [[[[[[[:a[[[[[:b]]]]]]]]]]][:c]])
(defet pathological-14 [[[[[[[[:a][[[[[:b]]]]]]]]]]][[:c]]])
(defet pathological-15 [[[[[[[[[:a]][[[[[:b]]]]]]]]]]][[:c]][]])
(defet pathological-16 [[[[[[[[[[:a]]][[[[[:b]]]]]]]]]]][[:c]][[]]])

(defet pathological-17 [[[[[[[:a[[[[[:b :x]]]]]]]]]]][:c 1]])
(defet pathological-18 [[[[[[[[:a][[[[[:b :x]]]]]]]]]]][[:c 2]]])
(defet pathological-19 [[[[[[[[[false :a]][[[[[:b :x]]]]]]]]]]][[:c 3]][]])
(defet pathological-20 [[[[[[[[[[false :a]]][[[[[:b :x]]]]]]]]]]][[:c 4]][[]]])

(defet pathological-21 [nil[[[[[[:a[[[[[:b :x]]]]]]]]]]][:c 1]])
(defet pathological-22 [[[[[[[[:a][[[[[:b Double/POSITIVE_INFINITY :x]]]]]]]]]]][[:c 2]]nil])
(defet pathological-23 [[nil nil[[[[[[[false [Double/NEGATIVE_INFINITY] :a]][[[[[:b :x]]]]]]]]]]][[:c 3]][nil]])
(defet pathological-24 [[[nil[[[[[[[false :a]]][[[[[:b :x]]]]]]]]]]][[:c 4]][[nil]]])

;;;; invalid path-type definition
(deftest try-to-overwrite-delim
  (is (thrown-with-msg? Exception #"attempted to define path type using the system-reserved hex-code 0x0"
                        (defpathtype [0x00 nil] str str))))



;;;; new encoding-type definition

(defmacro defet-new [name form]
  `(deftest ~name
     (let [form# ~form
           encoded# (encode form#)
           decoded# (decode encoded#)]
       (is (= form# decoded#) (str "encoding/decoding failed\n" "before: " form# "\nafter: " decoded#)))))

;; simple type test

;; Note: this is probably not a reasonable thing to do since ratios might be reduced to integers.
(defpathtype [0x28 clojure.lang.Ratio]
  str
  read-string)

(defet-new ratio-test-1 1/3)
(defet-new ratio-test-2 3/9)

;;;; encoding decoding errors

#_(deftest no-encoder-for-element
  (try
    (do
      (encode #{})
      (throw (ex-info "Test was supposed to throw an error" {:test 'no-encoder-for-element})))
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [cause type element]} (ex-data e)]
        (is (= cause :no-matching-encoder))
        (is (= type clojure.lang.PersistentHashSet))
        (is (= element #{}))))))

#_(deftest no-decoder-for-element
  (try
    (do
      (decode (str (char 0x2) #{:unrecognized} (char 0x00)))
      (throw (ex-info "Test was supposed to throw an error" {:test 'no-decoder-for-element})))
    (catch clojure.lang.ExceptionInfo e
      (let [{:keys [cause hex-code element-as-string]} (ex-data e)]
        (is (= cause :no-matching-decoder))
        (is (= hex-code "0x2"))
        (is (= element-as-string "#{:unrecognized}"))))))

;; record test

(defrecord Point [x y])

(defpathtype [0x100 codax.pathwise_test.Point]
  #(str (:x %) "x" (:y %))
  #(let [[x y] (map read-string (str/split % #"x"))]
     (Point. x y)))

(defet-new point-test-1 (Point. 100 200))
(defet-new point-test-1 (Point. -100 50))

(def pt1 (Point. 50 70))
(def pt2 (Point. 90 100))

(defet-new vec-of-new-types-1 [pt1 (Point. -999 4) (Point. 0 0) 1/5 2/8])
(defet-new vec-of-new-types-1 [pt1 [(Point. -999 4)] (Point. 0 0) [] 1/5 [[]] 2/8])
(defet-new vec-of-new-and-old-types [:a pt2 :b -1000 1/32 "string" 'symbol])


;;;; random path testing (old code)

(defn test-encoding [x & {:keys [hide-success hide-fail]}]
  (let [encoded (encode x)
        decoded (decode encoded)]
    (is (= decoded x))
    (if (= x decoded)
      (do
        (when (not hide-success) (logln "✓ -- " x))
        true)
      (do
        (when (not hide-fail)
          (logln "✗ FAIL")
          (pprint x)
          (logln "NOT EQUAL TO")
          (pprint decoded))
        false))))


(defn gen-random-string [& {:keys [max-length min-char max-char]}]
  (let [length (int (rand (or max-length 10)))
        min-char (or min-char 32)
        max-char (- (or max-char 127) min-char)]
    (reduce str "" (take length (iterate (fn [_] (char (+ min-char (int (rand max-char))))) nil)))))


(declare gen-random-vector gen-random-element gen-random-set gen-random-map)

(defn gen-random-element [& {:keys [max-length]}]
  (let [num (rand 16)]
    (cond
      (> num 14) (gen-random-map :max-length max-length)
      (> num 12) (gen-random-set :max-length max-length)
      (> num 10) (gen-random-vector :max-length max-length)
      (> num 9) nil
      (> num 8) true
      (> num 7) false
      (> num 6) (+ 0.25 (float (int (rand 10000))))
      (> num 5) (+ 0.25 (float  (int (- (rand 10000)))))
      (> num 4) (int (rand 10000))
      (> num 3) (- (int (rand 10000)))
      (> num 2) (gen-random-string);; :min-char 1 :max-char 65535 :max-length 100)
      (> num 1) (keyword (gen-random-string))
      :else (symbol (gen-random-string)))))


(defn gen-random-vector [& {:keys [max-length]}]
  (let [max-length (if max-length (dec max-length) 15)
        length (int (Math/floor (rand max-length)))]
    (if (> length 0)
      (vec (take (inc length) (rest (iterate (fn [_] (gen-random-element :max-length max-length)) nil))))
      [])))

(defn gen-random-set [& {:keys [max-length]}]
  (let [max-length (if max-length (dec max-length) 15)
        length (int (Math/floor (rand max-length)))]
    (if (> length 0)
      (set (take (inc length) (rest (iterate (fn [_] (gen-random-element :max-length max-length)) nil))))
      #{})))

(defn gen-random-map [& {:keys [max-length]}]
  (let [max-length (if max-length (dec max-length) 15)
        length (int (Math/floor (rand max-length)))]
    (if (> length 0)
      (->> (iterate (fn [_] (gen-random-element :max-length max-length)) nil)
           rest
           (take (inc length))
           (partition 2)
           (map vec)
           (into {}))
      {})))

(defn test-random [& {:keys [hide-success]}]
  (test-encoding (gen-random-vector) :hide-success hide-success))

(defn test-many-random [iterations & {:keys [hide-success]}]
  (let [hide-success (if hide-success
                       true
                       (> iterations 25))]
    (loop [n iterations
           suc 0]
      (if (zero? n)
        (do
          (logln "------------")
          (if (= suc iterations)
            (do (logln "Pathwise Test Success: " suc "/" iterations) true)
            (logln "Pathwise Test Failed: " suc "/" iterations)))
        (if (test-random :hide-success hide-success)
          (recur (dec n) (inc suc))
          (recur (dec n) suc))))))

(deftest random-path-test
  (is (test-many-random 100 :hide-success true)))

(deftest map-ordering-1
  (is (= (encode {:a 1, {:k :b} 2})
         (encode {{:k :b} 2 :a 1}))))

(deftest map-ordering-2
  (let [f1 {:a 1, {:k :b} 2}
        e1 (encode f1)
        d1 (decode e1)
        f2 {{:k :b} 2 :a 1}
        e2 (encode f2)
        d2 (decode e2)]
    (is (= f1 d1 f2 d2))))

(deftest set-ordering-1
  (is (= (encode #{#{:a :b} :c {:a 1}})
         (encode #{{:a 1} :c #{:b :a}}))))
