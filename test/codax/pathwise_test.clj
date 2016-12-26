(ns codax.pathwise-test
  (:require [codax.pathwise :refer :all]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]))

;;;; testing

(defn test-encoding [x & {:keys [hide-success hide-fail]}]
  (let [encoded (encode x)
        decoded (decode encoded)]
    (if (= x decoded)
      (do
        (when (not hide-success) (println "✓ -- " x))
        true)
      (do
        (when (not hide-fail)
          (println "✗ FAIL")
          (pprint x)
          (println "NOT EQUAL TO")
          (pprint decoded))
        false))))


(defn gen-random-string [& {:keys [max-length min-char max-char]}]
  (let [length (int (rand (or max-length 10)))
        min-char (or min-char 32)
        max-char (- (or max-char 127) min-char)]
    (reduce str "" (take length (iterate (fn [_] (char (+ min-char (int (rand max-char))))) nil)))))


(declare gen-random-vector gen-random-element)

(defn gen-random-element [& {:keys [max-vector-length]}]
  (let [num (rand 12)]
    (cond
      (> num 10) (gen-random-vector :max-length max-vector-length)
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
      (vec (take (inc length) (rest (iterate (fn [_] (gen-random-element :max-vector-length max-length)) nil))))
      [])))


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
          (println "------------")
          (if (= suc iterations)
            (do (println "Pathwise Test Success: " suc "/" iterations) true)
            (println "Pathwise Test Failed: " suc "/" iterations)))
        (if (test-random :hide-success hide-success)
          (recur (dec n) (inc suc))
          (recur (dec n) suc))))))

(deftest random-path-test
  (is (test-many-random 100 :hide-success true)))
