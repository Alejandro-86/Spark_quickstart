# Spark Quickstart

This is a quickstart guide to get you up and running with Apache Spark. This guide is intended for those who are new to Spark and want to get a feel for the basics of the Spark ecosystem. This guide will cover the following topics:

1. Install and set up Spark - Install Spark standalone on a machine, configure environment variables install PySpark using pip. 
2. Execute commands on the Spark interactive shell - Performing basic data read, write, and transform operations on the Spark shell. 
3. Use RDDs in Spark 2 - Performing in-memory transformations using lambdas, converting RDDs to Data Frames.
4. Use Data Frames in Spark 2 - Reading and writing data using Data Frames (Datasets in Scala).
5. Perform transformations and actions on data - Performing grouping, aggregations on data, ordering data.
6. Submit and run a job on a Spark cluster - Using spark-submit to run long-running jobs on a Spark cluster.
7. Create and use shared variables in Spark - Use broadcast variables and accumulators.
8. Monitor Spark jobs - view scheduler stages, tasks, executor information.
9. Run Spark on the Cloud - Set up Spark on Amazon EMR, Azure HDInsight, and Google Cloud Dataproc and run Spark jobs.


## Install and set up Spark

Clone the Spark repository from GitHub and cd into the Spark directory

```bash
git clone git@github.com:Alejandro-86/Spark_quickstart.git
cd Spark_quickstart
```

Install the required dependencies and verify the installation by entering the Spark shell

```bash
poetry install
poetry run pyspark
```

To exit the Spark shell, type `exit()` or press `Ctrl + D`