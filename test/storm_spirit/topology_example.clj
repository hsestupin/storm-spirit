(ns storm-spirit.topology-example
  (:import [backtype.storm StormSubmitter LocalCluster])
  (:use [backtype.storm clojure config]))

(defspout sentence-spout ["sentence"]
          [conf context collector]
          (let [sentences ["a little brown dog"
                           "the man petted the dog"
                           "four score and seven years ago"
                           "an apple a day keeps the doctor away"]]
            (spout
              (nextTuple []
                         (Thread/sleep 100)
                         (emit-spout! collector [(rand-nth sentences)])
                         )
              (ack [id]
                   ;; You only need to define this method for reliable spouts
                   ;; (such as one that reads off of a queue like Kestrel)
                   ;; This is an unreliable spout, so it does nothing here
                   ))))

(defspout sentence-spout-parameterized ["word"] {:params [sentences] :prepare false}
          [collector]
          (Thread/sleep 500)
          (emit-spout! collector [(rand-nth sentences)]))

(defbolt split-sentence ["word"] [tuple collector]
         (let [words (.split (.getString tuple 0) " ")]
           (doseq [w words]
             (emit-bolt! collector [w] :anchor tuple))
           (ack! collector tuple)
           ))

(defbolt word-count ["word" "count"] {:prepare true}
         [conf context collector]
         (let [counts (atom {})]
           (bolt
             (execute [tuple]
                      (let [word (.getString tuple 0)]
                        (swap! counts (partial merge-with +) {word 1})
                        (emit-bolt! collector [word (@counts word)] :anchor tuple)
                        (ack! collector tuple)
                        )))))

(defn new-topology []
  (topology
    {"1" (spout-spec sentence-spout)
     "2" (spout-spec (sentence-spout-parameterized
                       ["the cat jumped over the door"
                        "greetings from a faraway land"])
                     :p 2)}
    {"3" (bolt-spec {"1" :shuffle "2" :shuffle}
                    split-sentence
                    :p 5)
     "4" (bolt-spec {"3" ["word"]}
                    word-count
                    :p 6)}))