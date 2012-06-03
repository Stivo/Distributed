###This is the (yet unnamed) distributed DSL project.

####What is this?
This is a small DSL I am building as my master thesis at [EPFL](http://www.epfl.ch). 
It features a collection like interface,
and compiles currently to [Spark](http://spark-project.org/), [Scoobi](https://github.com/NICTA/scoobi) and experimentally to [Crunch](https://github.com/cloudera/crunch). 

Unlike most other pure library approaches, we can do more optimizations:
* Narrow the types used in the program to contain only fields used in later stages
* Move constant values out of the closures
* Fuse all flatMap, map and filter and apply optimizations across them

####Folder layout:
* dsl contains the implementation of the DSL
* virtualization-lms-core is the underlying framework, which deals with code motion and 
many other aspects of a compiler
* scoobi is the target for generated sources for scoobi
* spark is the target for generated sources for spark
* crunch is the target for generated sources for crunch
* pig contains the pig scripts we use as reference
* benchmarking contains some scripts to help with benchmarking
* utils contains small utilities
* project contains the sbt configuration

This library is not (yet) meant for public usage. It is more of a prototype than a correct implementation.

####Getting started:
The build should be reproducible now.
For building you will need:
* sbt 0.11.3 launcher
* git
* mvn3 for the crunch build

To get started, run setup.sh. This will fetch spark and crunch and deploy them to the local repository. Make sure that spark and crunch have been deployed correctly.
When this is done, you can start sbt in the top distributed folder. For generating the classes you can use:
	~ ; dsl/test ; dotgen ; gens/compile
This will regenerate all test classes, update the utils, generate dot files and compile the generated files.

For benchmarking and packaging, I use the scripts in benchmarking. They are pretty messy though. Install Cloudera cdh3u4 on your machine to test the generated crunch and scoobi programs.
