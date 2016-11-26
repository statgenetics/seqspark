# SEQSpark Manual

Zhang Di & Suzanne M. Leal

[TOC]

## Introduction

SEQSpark analyzes large-scale genotype data both in the sense of sample size and of number of variants, usually generated from Whole-Genome-Sequencing (WGS) or Exome-Sequencing (ES) project. In this manual, we will describe its functionalities in data quality control and association testing.

### Prerequisites

Since SEQSpark is based on Apache Spark, so to use it in any non-trivial situation you need a running Spark cluster, either provided by your organization or built by yourself. If you want to build a Spark cluster yourself, please refer to [Hadoop](https://hadoop.apache.org/) and [Spark](http://spark.apache.org). If you do not have proper hardware onsite, you may want to setup a cloud environment in AWS, for which we provide a template configuration. We also provide a local Spark configuration for you to play with. Please do not run anything except the demos in the local configuration.

To build SEQSpark from source, you need `sbt` and `gfortran` installed.

### Getting Started

In a HOCON format configuration file, you specify the paths of the input genotype file (VCF) and a PED-like phenotype file, and the pipeline with its parameters you want to run. HOCON is an abbreviation for Human-Optimized-Config-Object-Notation. As the name suggested, it is very intuitive to understand and easy to use. Please refer to Examples and Specification for more details. To minimize your typing, we embedded reasonable defaults for all the parameters also in a HOCON file which comes with SeqA. You only need to configure the different parts in your own file. With the configuration file and all the necessary input files ready, you can run your analysis like this: 

```shell
cluster:~ user$ SEQSpark SingleStudy seqa.conf [spark-options]
```

As with other Spark-based applications, you can specify useful Spark options like “num-executors” on command line. For more options in Spark, please refer to submitting-applications. In section Configuration, we will explain the options in the configuration file section by section. In section Examples, we will give several examples on how to use it in various scenarios.

## Installation

### Binary version

SEQSpark is a Spark application written in Scala, so the binary is simply a jar file with a few assistant scripts. You can download the whole package from [here](http://SEQSpark.dizhang.org/SEQSpark.tar):

```shell
user@server ~$ wget http://SEQSpark.dizhang.org/SEQSpark.tar
```

After you downloading the tar file, you can uncompress it:

```shell
user@server: ~$ tar xf SEQSpark.tar
```

Move the folder to somewhere desirable:

```shell
user@server: ~$ mv SEQSpark ~/software/
```

Set the path to your environment:

```shell
user@server: ~$ echo "PATH=~/software/SEQSpark:$PATH" >> ~/.bashrc
```

The last two steps are optional, which can usually help you organize your packages and faciliate the use.

This binary version only includes the program, you then need to download the databases:

```shell
user@server: ~$ SEQSpark-db ref RefSeq,dbSNP
```

The RefSeq and dbSNP databases will be downloaded to the remote ref folder of your HDFS.

### Build from source

To build SEQSpark from source, you need `sbt`, which is available [here](http://www.scala-sbt.org/0.13/docs/Manual-Installation.html). And because we used one fortran program to calculate the multi-variate normal distribution, you also need `gfortran`, which should come with `gcc` on most linux distros.

Get the source code:

```shell
user@server: ~$ git clone https://github.com/zhangdi-devel/SEQSpark.git
```

If you don't have git, just paste the URL to your web browser and go to the website to download the zip file.

Compile the source and install:

```shell
user@server: SEQSpark$ ./install --prefix ~/software/SEQSpark --db-dir ref 
```

If you have never run `sbt` or compiled `SEQSpark` before, it may take some time to download the dependencies. This `install` script will not only compile the source code, but also download RefSeq and dbSNP database files. For other prebuilt databases, you need to use the script `SEQSpark-db`. The CADD database is quite large (65GB), so it may take some time to finish. For a full description of the scripts, please refer to section Scripts. 

### Virtual machine demo

We built a virtual machine demo for you to play with SEQSpark. Please download this Vagrant [configuration file](https://SEQSpark.dizhang.org/Vagrantfile). You need Vagrant and VirtualBox installed on your host OS. Please check the Vagrant download [page](https://www.vagrantup.com/downloads.html) and the VirtualBox download [page](https://www.virtualbox.org/wiki/Downloads) for your platform (Mac, Windows, or Linux). Once you have them installed, just type `vagrant up` in the same folder containing the configuration file. Then you can log into the virtual machine by typing `vagrant ssh`.

In the virtual machine, you can run the demos of SEQSpark:

```shell
user@sepspark: ~$ SEQSpark SingleStudy SEQSpark-demo.conf
```

## Configuration

The configuration file contains all the options of SEQSpark. 

### Single study

Under the root braces of the configuration file, you can set up general runtime options for a certain submission.

```json
project = SEQSpark
localDir = ${PWD}
dbDir = SEQSpark_db
pipeline = [ qualityControl, association ]
```

### Meta analysis