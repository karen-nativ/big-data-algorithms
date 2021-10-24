# Implementions in Hadoop and Giraph
Implementations of solutions for two big-data problems; Graph triangle counting, and F1 frequency moments.

This is part of an essay in which we compare the MapReduce and Pregel algorithms.

----------------------
Prerequisites
----------------------
In order to run these implementations the following are required:
* Hadoop - the framework, declared environment variables and ssh permissions
* Giraph - the framework, Maven automation tool(for easier compilation and running)


----------------------
Theory
----------------------
In short -
MapReduce is an algorithm built of two main steps (`map()` & `reduce()`)
Users define these functions and the data processing is therefore split to two stages.

Pregel is an algorithm based on graph computing.
Users define a `compute()` function that is run by each vertex in the graph, and messages are sent between vertices in a series of supersteps.

----------------------
File Hierarchy
----------------------
The repository contains implementations for two big-data problems in each framework.
Each framework is contained in a directory.
The structure of each such directory is:
|-<Framework>
      |- input
          |- <input test cases>
          |- ...
      |- src
          |- <java files>
          |- ...
