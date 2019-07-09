# Deutsche-Borse-Dataset-based-Spark-Project

This is a Spark/Scala based project analyzing the publicly available Deutsche Borse Dataset (https://github.com/Deutsche-Boerse/dbg-pds)

## Dataset: 
s3://deutsche-boerse-xetra-pds (for Xetra data)

## Technology: 
Scala/Spark

## Data Dictionary for the Dataset:
https://github.com/Deutsche-Boerse/dbg-pds/blob/master/docs/data_dictionary.md

## Source Files:
Project has three main source file as below:
- BiggestWinner.scala
- ImpliedVolume.scala
- MostTraded.scala
## Steps to Execute this Project:
1. Change the Input/Output file paths in the three source files
2. Compile the code using the sbt package command in the base folder of the project
3. Run the project using the spark-submit command (from the bin folder where spark is installed)
