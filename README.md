###This is the (yet unnamed) distributed DSL project.

####What is this?
This is a small DSL I am building as my master thesis at [EPFL](http://www.epfl.ch). 
It features a collection like interface,
and compiles currently to [Spark](http://spark-project.org/) and [Scoobi](https://github.com/NICTA/scoobi). 

Unlike most other pure library approaches, we can do more optimizations:
* Narrow the types used in the program to contain only fields used in later stages
* Move constant values out of the closures

####Layout of this project:
* dsl contains the implementation of the DSL
* virtualization-lms-core is the underlying framework, which deals with code motion and 
many other aspects of a compiler
* scoobi is the target for generated sources for scoobi
* spark is the target for generated sources for spark
* benchmarking contains some scripts to help with benchmark
* utils contains small utilities
* project contains the sbt configuration

This library is not (yet) meant for public usage. It is currently in alpha

####How is this meant to be used?
Currently I do this:  
sbt in the top folder, a command like:  
    ~ ; dsl/test ; dotgen ; scoobi/compile ; spark/compile
during development.  
For benchmarking and packaging, I use the scripts in benchmarking. 