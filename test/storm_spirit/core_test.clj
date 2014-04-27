(ns storm-spirit.core-test
  (:require [clojure.test :refer :all]
            [storm-spirit.core :refer :all])
  (:use [storm-spirit.topology-example]))

;(deftest a-test
;  (testing "FIXME, I fail."
;    (is (= 0 1))))

(defn v1 []
  (graphviz (new-topology)))

(defn v2 []
  (graphviz (new-topology) {:rankdir :LR}))


