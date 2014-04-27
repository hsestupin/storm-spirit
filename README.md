# storm-spirit

A Clojure library designed to help you better understand your <a href="https://github.com/apache/incubator-storm">Storm</a> topology visualizing it.
At this moment the only one visualization way is supported - <a href="http://www.graphviz.org/">graphviz</a>.

## Usage

Assume that var `topology` holds your topology built something
<a href="https://github.com/hsestupin/storm-spirit/blob/master/test/storm_spirit/topology_example.clj#L45">like that</a>.
For building graph views you have to install graphviz tool (http://www.graphviz.org/Download.php).
Further just execute the following code:

``` clojure
(storm-spirit.core/graphviz topology)
```

Also by default it draws vertical graph but you could specify the direction like this (see http://www.graphviz.org/content/attrs#drankdir):

``` clojure
(storm-spirit.core/graphviz {:rankdir :LR})
```