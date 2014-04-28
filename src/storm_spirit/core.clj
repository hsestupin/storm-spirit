(ns storm-spirit.core
  "You always wanted to visualize your topology using dot.  Now you can"
  (:import [backtype.storm.generated Bolt SpoutSpec Grouping StormTopology ComponentCommon GlobalStreamId])
  (:import [java.io File]
           [java.net URL])
  (:use [backtype.storm thrift util])
  (:require [clojure.java
             [shell :as sh]
             [browse :as browse]]
            [clojure.string :as str]
            [dorothy.core :as dorothy]))

(def ^{:private true}
  colors
  ["red" "yellow" "green" "blue" "purple" "orange" "brown" "gray" "cyan"
   "magenta" "violet" "pink" "goldenrod" "lawngreen" "cadetblue" "deeppink"
   "darkolivegreen"])

(defn- get-color
  "returns a color based on the object's hashcode"
  [obj]
  (colors (mod (.hashCode obj) (count colors))))

(defn- nice-class-name
  [obj]
  (str/replace (type obj) (re-pattern "^.*\\.") ""))

(defn- label
  "labels are id (class) parallelism"
  [obj]
  (let [obj-type (nice-class-name
                   (deserialized-component-object
                     (condp = (type (val obj))
                       Bolt (.get_bolt_object (val obj))
                       SpoutSpec (.get_spout_object (val obj)))))
        p-hint (.. (val obj) get_common get_parallelism_hint)
        p (if (= 0 p-hint) 1 p-hint)]
    {:label (str (key obj) " (" obj-type ") p=" p)}))

(defn spout-nodes
  "spouts are squares"
  [spouts]
  (map (fn [spout]
         [(key spout) (conj (label spout) {:shape :box})])
       spouts))

(defn- bolt-nodes
  "bolts are circles"
  [bolts]
  (map (fn [bolt]
         [(key bolt) (label bolt)])
       bolts))

(defn- escape-quotes
  [s]
  (str/escape s {\\ "\\\\", \" "\\\""}))


(defn- edge-label
  [stream-id grouping]
  (let [grouping-str (case (.getFieldName (.getSetField grouping))
                       "fields" (str (.get_fields grouping))
                       (str (.getFieldName (.getSetField grouping))))
        stream-name (if (not= stream-id "default")
                      (str " \\\"" stream-id "\\\""))]
    (conj
      (if (= stream-id "default")
        {}
        {:color (get-color stream-id)})
      {:label (str (escape-quotes grouping-str) stream-name)})))

(defn input-to-edge
  "Build edge vector from bolt's input to bolt"
  [bolt-id bolt-input]
  (let [^GlobalStreamId from (key bolt-input)
        ^Grouping grouping (val bolt-input)
        input-id (.get_componentId from)
        stream-id (.get_streamId from)]
    [input-id bolt-id
     (conj
       (if (= stream-id "default")
         {}
         {:color (get-color stream-id)})
       (edge-label stream-id grouping))]))

(defn bolt-to-edges
  "Returns vector of edges from bolt inputs to the bolt"
  [bolt-spec]
  (let [bolt-id (key bolt-spec)
        bolt-inputs (->> (val bolt-spec)
                         (.get_common)
                         (.get_inputs))]
    (vec (map #(input-to-edge bolt-id %) bolt-inputs))))

(defn edges
  "Returns all graph edges according to bolt specifications"
  [bolts]
  (mapcat bolt-to-edges bolts))

(defn topology-to-dot
  "Takes a topology and converts it to a dot digraph string representation"
  [^StormTopology topology graph-opts]
  (let [spouts (.get_spouts topology)
        bolts (.get_bolts topology)]
    (dorothy/dot (dorothy/digraph
                   {:id "topology"}
                   (concat
                     [(dorothy/graph-attrs graph-opts)]
                     (spout-nodes spouts)
                     (bolt-nodes bolts)
                     (edges bolts))))))

(defn dot-to-png ^URL
  ([dot-str ^File file]
   (print "Printing graph: \n" dot-str)
   (let [file-name (.toString file)
         png-file-name (str file-name ".png")]
     (spit file dot-str)
     (let [output (sh/sh "dot" "-Tpng" "-o" png-file-name file-name)]
       (if (not= 0 (:exit output))
         (throw (RuntimeException. (:err output)))
         (.toURL (.toURI (File. png-file-name)))))))
  ([dot-str]
   (dot-to-png dot-str (File/createTempFile "-fs-" ".dot"))))

(defmulti visualize :view-format)

(defmethod visualize :graphviz
  [{:keys [topology graph-attrs]}]
  (browse/browse-url
    (str (dot-to-png
           (topology-to-dot
             topology graph-attrs)))))

(defn visualize-with-graphviz [topology & [args]]
  (visualize
    (conj {:view-format :graphviz :topology topology} args)))


; DEV
;(import '(backtype.storm StormSubmitter LocalCluster))
;(use '(backtype.storm clojure config))
;
;(defspout sentence-spout ["sentence"]
;          [conf context collector]
;          (let [sentences ["a little brown dog"
;                           "the man petted the dog"
;                           "four score and seven years ago"
;                           "an apple a day keeps the doctor away"]]
;            (spout
;              (nextTuple []
;                         (Thread/sleep 100)
;                         (emit-spout! collector [(rand-nth sentences)])
;                         )
;              (ack [id]
;                   ;; You only need to define this method for reliable spouts
;                   ;; (such as one that reads off of a queue like Kestrel)
;                   ;; This is an unreliable spout, so it does nothing here
;                   ))))
;
;(defspout sentence-spout-parameterized ["word"] {:params [sentences] :prepare false}
;          [collector]
;          (Thread/sleep 500)
;          (emit-spout! collector [(rand-nth sentences)]))
;
;(defbolt split-sentence ["word"] [tuple collector]
;         (let [words (.split (.getString tuple 0) " ")]
;           (doseq [w words]
;             (emit-bolt! collector [w] :anchor tuple))
;           (ack! collector tuple)
;           ))
;
;(defbolt word-count ["word" "count"] {:prepare true}
;         [conf context collector]
;         (let [counts (atom {})]
;           (bolt
;             (execute [tuple]
;                      (let [word (.getString tuple 0)]
;                        (swap! counts (partial merge-with +) {word 1})
;                        (emit-bolt! collector [word (@counts word)] :anchor tuple)
;                        (ack! collector tuple)
;                        )))))
;
;(defn new-topology []
;  (topology
;    {"1" (spout-spec sentence-spout)
;     "2" (spout-spec (sentence-spout-parameterized
;                       ["the cat jumped over the door"
;                        "greetings from a faraway land"])
;                     :p 2)}
;    {"3" (bolt-spec {"1" :shuffle "2" :shuffle}
;                    split-sentence
;                    :p 5)
;     "4" (bolt-spec {"3" ["word"]}
;                    word-count
;                    :p 6)}))
;
;(def t (new-topology))
