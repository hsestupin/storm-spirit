(ns storm-spirit.core-test
  (:require [clojure.test :refer :all]
            [storm-spirit.core :refer :all])
  (:use [storm-spirit.topology-example]))

(def t (new-topology))

(deftest label-node-test
  (testing "FIXME, I fail."
    (is (= 0 1))))

(defn v1 []
  (visualize-with-graphviz t))

(defn v2 []
  (visualize-with-graphviz t {:rankdir :LR}))


