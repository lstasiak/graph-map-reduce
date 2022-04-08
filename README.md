# graph-map-reduce

The repository contains an implementation of class representing a graph with function that calculates local clustering coefficients for every node in a graph and (based on that) average clustering coefficient.
 - The code is based on spark framework and RDD structures. 
 - Input dataset consists of graph edges in a form of `(Int, Int)` set of tuples. 
 - The local clustering coefficient formula is based on triangle counting algorithm `NodeIterator++` in a Map Reduce way. 

What is need to do next? 
  - improve code performance, as now it's not a great solution. 
