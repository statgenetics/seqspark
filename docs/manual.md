# SEQSpark Manual

Copyright 2016-2018 Zhang Di & Suzanne M. Leal

[TOC]

## 1. Getting started

SEQSpark was developed to analyze large-scale genotype data that consists of many samples and a large number of variants generated from Whole-Genome-Sequencing (WGS) or Exome-Sequencing (ES) project. SEQSpark can also analyze imputed genotype data. In this manual, is described SEQSpark's functionalities in performing anotation, data quality control and association testing.

### 1.1. Prerequisites

The comiple tool `sbt` is needed to build SEQSpark. If it is not available in your environment, you can download it [here](http://www.scala-sbt.org/).

Since SEQSpark is based on [Spark](http://spark.apache.org/), to use it you need a computational environment which can run Spark. If you don't have a ready-to-use Spark server/cluster, you can follow the guides in appendices for installation. For more details, please refer to the official websites of [Hadoop](https://hadoop.apache.org/) and [Spark](http://spark.apache.org).

### 1.2. Installation

To obtain the source code:

```shell
dolores@cluster: ~$ git clone https://github.com/statgenetics/seqspark.git
```

To compile the source code and install:

```shell
dolores@cluster: seqspark$ ./install --prefix ~/software/seqspark --db-dir /user/dolores/seqspark
```

If you have never compiled `SEQSpark` before, it may take some time to download its dependencies. By default, the `install` script not only compiles the source code, but also downloads the RefSeq and dbSNP databases and puts them to HDFS. For other prebuilt databases, you need to use the script `seqspark-db`. The CADD database is quite large (65GB), so depending on the network connection it may take quite some time to download.

### 1.3. How to run SEQSpark

In the configuration file, specify the paths of the input genotype file (VCF) and a phenotype file (TSV), and the pipeline with the parameters you want to run. With the configuration file and all the necessary input files ready, you can perform your analysis as follows: 

```shell
dolores@cluster: ~$ seqspark some.conf [spark-options]
```

We will explain the parameters that can be set in the configuration file, and how to use them to perform data quality control, annotation and association tests in the following chapters.

## 2. Tutorial

In this tutorial, we will guide you in the analysis of a simulated dataset of 2000 exomes. It will cover a large range of (but not all) parameters that can be controlled in the configuration file. For a detailed description, please refer to Chapter 3.

### 2.1. Preparation

After `SEQSpark` is installed as described in 1.2. 

Download the dataset and upload the data into the Hadoop file system (HDFS) for analysis using the `put` command:

```shell
dolores@cluster: demo$ wget http://seqspark.statgen.us/simulated.vcf.bz2
dolores@cluster: demo$ hdfs dfs -put simulated.vcf.bz2
dolores@cluster: demo$ wget http://seqspark.statgen.us/simulated.tsv
dolores@cluster: demo$ hdfs dfs -put simulated.tsv
```

### 2.2. Basic quality control

Modify the configuration file to specify the desired QC pipeline, adding more parameters as the QC is performed.

**Filter genotypes based on DP and GQ score.**

First, create a configuration file:

```shell
dolores@cluster: demo$ vim demo.conf
```

In the demo file, type or paste the following contents:

```json
seqspark {
  project = demo
  pipeline = [ "qualityControl" ]
  input {
  	genotype.path = "simulated.vcf.bz2"
  	phenotype.path = "simulated.tsv"
  }
  qualityControl {
    genotypes = ["DP >= 8 and GQ >= 20"]
  }
}
```

Below is an explanation of each of the files parameters:

`seqspark {}` is mandated and anything outside the curly brackets will be ignored.

`project` is the name of the run, and it will be used as a suffix when creating cached dataset in HDFS.

`pipeline` is an array of procedures to be performed, here we will only perform the `qualityControl` procedure, and its parameters are specified below.

`input` is the configuration for genotype and phenotype data.  You will need to specify the path for the location of these files.  By default the location of the files are within HDFS, and the path refers to the location with HDSF.  Usually large files, e.g. genotypes are stored within HDFS for the analysis. You can also specify a local path for example the phenotype file as fellows  `"file:///home/dolores/demo/simulated.tsv"`. It should be noted that only smaller sized files such as the phenotype file should be stored locally on the server/cluster.  

`qualityControl`  contains the parameters for the quality control procedures. In addition to the expression `qualityControl {genotypes = ["DP >= 8 and GQ >= 20"]}` the term can be expressed as 

`qualityControl.genotypes=["DP >= 8 and GQ >= 20"]`.  Additionally instead including `"and"` between each filter they can simply be listed as follows  `genotypes = ["DP >= 8", "GQ >= 20"]`.

**Filter variants** 

Modify the configuration file as follows:

```shell
seqspark {
  project = demo
  pipeline = [ "qualityControl" ]
  input {
  	genotype.path = "simulated.vcf.bz2"
  	phenotype.path = "simulated.tsv"
  }
  qualityControl {
    genotypes = ["DP >= 8 and GQ >= 20"]
    variants = ["missingRate <= 0.1"]
  }
}
```

The only difference here compared to above that an additional filter was added to remove variants are missing 10% or more of their genotype data.  Other filters such as `hwePvalue` can also be used here.

**Generate summaries for genotype data**

Modify the configuration file as follows:

```shell
seqspark {
  project = demo
  pipeline = [ "qualityControl" ]
  input {
  	genotype.path = "simulated.vcf.bz2"
  	phenotype.path = "simulated.tsv"
  }
  qualityControl {
    genotypes = ["DP >= 8 and GQ >= 20"]
    variants = ["missingRate <= 0.1"]
    summaries = ["pca", "titv"]
  }
}
```

This will perform Principal Component Analysis (PCA) of the genotype data, and calculate Ti/Tv ratios for the variants. Here the default minor allele frequency (MAF) of selecting variants with a MAF>0.01 is used and the default for the number of principal components is 10.  Later it will be explained how to select variants with other frequencies as well as prune variants for intermarker linkage disequilbrium (LD). The principal components can be used for data quality control and also loaded to control for substructure and admixture when performing association testing. The results are stored in the output/pca.txt and output/titv.txt files. 

### 2.3. Single variant association testing

To perform association testing, we need to add another section `association` to the configuration file:

```shell
seqspark {
  project = demo
  pipeline = [ "qualityControl", "association" ]
  input {
  	genotype.path = "simulated.vcf.bz2"
  	phenotype.path = "simulated.tsv"
  }
  qualityControl {
    genotypes = ["DP >= 8 and GQ >= 20"]
    variants = ["missingRate <= 0.1"]
    summaries = ["pca", "titv"]
  }
  association {
    trait {
      list = ["bmi", "disease"]
      bmi {
        binary = false
        covariates = ["sex", "age", "disease"]
        pc = 0
      }
      disease {
        binary = true
        covariates = ["sex", "age", "bmi"]
        pc = 0
      }
    }
    method {
      list = [snv]
    }
  }
}
```

Although it looks lengthy at first, the structure is clear and simple. First, change to the pipeline list to include `association`. The added  `association` section consists of two subsections, `trait` and `method`. 

`trait` describes the phenotypes to be analyzed and the corresponding covariates to include in the regression model.  It needs to be specified if the trait is binary (false would denote that the trait is quantitative), and how many principal components to include in the regression analysis. Here a binary trait disease and a quantitative trait BMI are being analyzed. 

`method` describes the methods to use. For single variant associaton analysis  `snv` is used to denote the type of analysis which should be performed. For the single variant association analysis currently the additive test is supported. 

### 2.4. Rare variant aggregate tests

To perform rare variant aggregate tests, the methods are added to the list as shown below:

```shell
seqspark {
  project = demo
  pipeline = [ "qualityControl", "association" ]
  input {
  	genotype.path = "simulated.vcf.bz2"
  	phenotype.path = "simulated.tsv"
  }
  qualityControl {
    genotypes = ["DP >= 8 and GQ >= 20"]
    variants = ["missingRate <= 0.1"]
    summaries = ["pca", "titv"]
  }
  association {
    trait {
      list = ["bmi", "disease"]
      bmi {
        binary = false
        covariates = ["sex", "age", "disease"]
        pc = 0
      }
      disease {
        binary = true
        covariates = ["sex", "age", "bmi"]
        pc = 0
      }
    }
    method {
      list = ["snv", "cmc", "brv", "vt", "skat", "skato"]
    }
  }
}
```

In the `list` besides `snv` is now included five rare variant aggregate tests: `cmc` the Combined Multivariate Collapsing; `brv` Burden of Rare Variants;  `vt` Variable Threashold;  `skat` Sequence Kernel Assocation Test; and `skato` SKAT-optimal. It should be noted that for rare variant analysis annotation using refseq is performed before the analysis.  By default all splice site, frameshift, missense and nonsense variants for each gene are analyzed that have a $MAF<0.01$.  It is possible to annotate using additional databases such as CADD and specfiy which variants to analyze e.g. $c.score >10$.  Also the user can specify the MAF cut-off to be used in the analysis.  The specifics will be covered later in the documentation. 

### 2.5. Running SEQSpark and collecting the results

Now that the configuration file is ready, it can be run with `SEQSpark`:

```shell
dolores@cluster demo$ seqspark demo.conf
```

In the current directory, an `output` directory with be created, and there you can find:

`titv.txt` summarizing the Ti/Tv ratio for each sample and for the dataset.

`pca.txt` contains the principal components values for each sample's genotypes for the 10 components.

A series of `assoc_$trait_$method` files contains the results of the association analysis for each trait and method combination.

## 3. Reference of configurations

### 3.1. root

```json
seqspark {
	project = seqspark
	pipeline = [ "qualityControl", "association" ]
  	partitions = 1024
}
```

`project`: The name of your project

`pipeline`: The pipeline to run

`partitions`: The number of spark partitions to use.

### 3.2. input

```json
seqspark {
  input {
    genotype {
      format = vcf
  	  path = ${seqspark.project}.vcf 
	  filters = [] 
	  variants = all 
    }
	phenotype {
      path = ${seqspark.project}.tsv
	  batch = none
	}
  }
}
```

`genotype.format`: The format of the input genotype file, can be either `vcf` or `impute2`. When `impute2` data is used, the analysis will be based on dosages for both single variant and rare aggregate association analysis.

`genotype.path`: The path in HDFS for the genotype data file.

`genotype.filters`:

- For VCF format, filter variants based on the FILTER and/or INFO columns of the VCF file. e.g. `filters = ["FILTER == \"PASS\"", "INFO.AN > 1800"]`
- For impute2 format, filter variants based on the INFO score. e.g. `filters = ["INFO >= 0.8"] `

`genotype.variants`: Load variants by regions. Valid values are


1. "all": default, load all variants.
2. "exome": only load coding variants. Very useful for rare variant aggregate tests using whole-genome data.
3. comma separated regions: only load variants in these regions. e.g. `"chr1"`,`"chr1:1-30000000,chr2"` 


`phenotype.path`: The path on HDFS or local filesystem for the phenotype data file.

`phenotype.batch`: To specify the batch when genetic data was generated in batches. Some quality control can be performed by batches to investigate and/or correct for batch specific artifacts or effects. The default is `none`, which means no batch information available.

### 3.3. qualityControl

```shell
seqspark {
  qualityControl {
    genotypes = []
    variants = []
    summaries = []
    pca {
      variants = [
          "informative",
          "missingRate <= 0.1",
          "chr != \"X\"",		
          "chr != \"Y\"",		
          "maf >= 0.01 and maf <= 0.99",
          "hwePvalue >= 13-5"
      ]
      noprune = true
      prune {
        window = 50
        step = 5
        r2 = 0.5
      }
    }
    save = false
    export = false
  }
}
```

`genotypes`: The filters for genotypes.

`variants`: The filters for variants.

- `chr`: chromosome
- `maf`: alternative allele frequency
- `informative`: not a monomorphic site
- `missingRate`: overall missing rate
- `batchMissingRate`: the maximum batch specific missing rate
- `batchSpecific`: the batch specific variants cutoff
- `isFunctional`: the variant site is functional i.e. splice site, missense, frameshift and nonsense
- `hwePvalue`: the P value of HWE test.

`summaries`:  functions to perform for genotype data which include

- `pca`: perform PCA analysis
- `titv`: calculate Ti/Tv ratio

`pca`: how to prepare data for PCA

- `pca.variants`: variants inclusion standard for PCA. For example exclude variants on the X and Y chromosomes



- `pca.noprune`: do not prune SNP based on LD
- `pca.prune`: LD prune parameters, similar to the PLINK pare-wise LD pruning.

`save`: save the clean dataset to HDFS in internal format

`export`: export the clean dataset to HDFS in VCF format

### 3.4. association

The association section contains two subsections, `trait` and `method`.

```json
seqspark {
  association {
    trait {
      list = [someTrait]
      someTrait {
      	key1 = value1
      	...
      }
    }
    method {
      list = [someMethod]
      someMethod {
	    key1 = value1
      	...
      }
    }
  }
}
```

#### 3.4.1. trait

`trait` contains a `list` of phenotypes to analyze and the parameters for each phenotype.

```json
someTrait {
  binary = false #true
  covariates = ["c1", "c2", ...]
  pc = 0
}
```

`binary`: `true` for binary trait and `false` for quantitative trait

`covariates`: a list of covariates in the phenotype file to be included in the regression model 

`pc`: number of principal components to be included in the regression model

#### 3.4.2. method

`method` contains a `list` of methods to use and the parameters for each method.

**Single variant test**

```json
snv {
  test = score #wald
  type = snv
  resampling = false
  maf {
    source = "pooled"
    cutoff = 0.01
  }
}
```

For single variant association analysis, the following commands can be used

`test`: Can either perform the score or Wald test

`type`: must be `snv` for single variant test

`resampling`: generate empirical p value when true or analytical p-values when false

`maf.source`: which source to use to calculate the minor allele frequency. This can be from a database such as ExAC or gnomAD

- `"pooled"`: using all samples
- `"controls"`: using only controls, this is only valid when the trait is binary, otherwise pooled sample will be used. It is not recommened to use controls since it has been shown to increase type I error.
- `"some_key"`: using `"some_key"` to extract value from the INFO field. The value must be a number between 0 and 1. otherwise, pooled sample will be used.

`maf.cutoff`: only perform analysis for variants with $MAF \ge$ cutoff. Single variant association tests have very low power for rare variants.

**Burden tests**

```json
burden {
  test = score #wald
  type = cmc #brv
  resampling = false
  weight = none #wss
  maf = {
    source = "pooled"
  	cutoff = 0.01
  	fixed = true
  }
  misc {
    varLimit = [ 2, 2000 ]
    groupBy = ["gene"]
    variants = ["isFunctional"]
    weightBeta = [1, 25]
  }
}
```

The `resampling` and `maf.source` options are the same as for the single variant analysis. The `maf.cutoff` specificies the frequency of the variants to be used in the analysis  $MAF <$ cutoff unlike for `snv` analysis it is of interest to analyze rare variants. 

`type`: the coding style.

- `cmc`: Combined Multivariate Collapsing method - uses binary coding - (1) one or more rare variants are included in the region or (0) no rare variants
- `brv`: Burden of Rare Variants - analyzes the number of variants within a region

`weight`: Variant weights can be used for all tests except for CMC

- `wss`: $\frac{1}{\sqrt{MAF \times {(1-MAF)}}}$
- `none`: no weight, or the weights are equal for all variants
- `skat`: weighted style for SKAT - $Beta(MAF, a_1, a_2)$, where $a_1$ and $a_2$ are 1 and 25 by default, and can be specified in `misc`.
- `"some_key"`: use `"some_key"` in the INFO field of the VCF

`maf.fixed`: use fixed MAF cutoff to perform the `cmc` or `brv` or to perform the variable threshold (VT) - set maf.fixed to false.  The type of coding which can be used for VT can be either `cmc` or `brv`.  For VT, `resampling` will be set to true as the default.

`misc`: more options for rare variant tests

- `misc.varLimit`: skip regions/genes with variants  less than the lower bound and more than the upper bound.  The default is varLimit = [ 2, 2000 ]
- `misc.groupBy`: group variants by genes. Can also be grouped by  `["slidingWindow", size, overlap]` or other regulatory regions.  For example using annotation from ENCODE
- `misc.variants`: only include variants that satisfy these filters. Annotations from various bioinformatic tools/databases can be used. e.g. `["CADD.score >= 3"]`
- `misc.weightBeta`: the $a_1$ and $a_2$ values for skat style weight

**SKAT/SKAT-O tests**

```json
skato {
  type = skato #skat
  weight = skat
  maf {
    source = pooled
    cutoff = 0.01
  }
  misc {. 
    varLimit = 2000
    groupBy = ["gene"]
    variants = ["isFunctional"]
    method = "optimal.adj"
    rhos = []
    kernel = "linear.weighted"
    weightBeta = [1, 25]
  }
}
```

`type` can be `skat` or `skato`. `weight` and `maf` are similar to burden tests, except that set `maf.fixed = false` and `test` has no effect. In the `misc`, `method`, `rhos`, `kernel`, and `weightBeta` are similar to their counterparts in the original SKAT R package. For more detail please see the R package documentation.

**Summary statistics for meta-analysis**

```json
sum {
  type = sum
  maf.source = pooled
  misc {
    varLimit = 2000
  	groupBy = ["slidingWindow", 1000000, 0]
  	variants = []
  }
}
```

Set `type = sum` to generate the score statistics and the covariance matrix. Other parameters are similar to the above association analysis methods. By default, set `groupBy = ["slidingWindow", 1000000, 0]` to generate summary statistics for variants in every 1 Mbp region. The summary statistics can later be used to perform single variant or rare variant aggregate meta-analysis. 

### 3.5. meta-analysis

In addition to performing association analysis for a single study, SEQSpark can perform meta-analysis using summary statistics generated from single study analyses.

```json
seqspark {
  project = someName
  partitions = 1024
  meta {
    studies = ["study1", "study2"]
    study1 {
      format = "seqspark"
      path = "hdfs:///user/dolores/data/study1.meta"
    }
    study2 {
      format = "seqspark"
      path = "hdfs://user/dolores/data/study2.meta"
    }
    method {
      list = ["methodName"]
      methodName {
        key1 = value1
        ...
      }
    }
  }
}
```

`project`: project name, same as the single study analysis

`partitions`: number of Spark partitions

`meta`: parameters controls how to perform meta-analysis

- `studies`: a list of studies
- `someStudy`: contains format and path of the summary statistics of the study
  - `someStudy.format`: now only supports the results from SEQSpark single study analysis, but will be expanded to additional formats
  - `someStudy.path`: the location of the summary statistics


- `method`: parameters controls the methods to use
  - `method.list`: a list of methods to use, e.g. SKAT, BRV etc. 
  - `method.methodName`: The name of the methods is provided, e.g. `method.skat`. The analysis used for for meta-analysis is similiar to single study association analysis with the exception: permuation based analysis cannot be used, since it is based on summary statistics, e.g. VT, is not available. 

To run meta-analysis, use the command as follows:

```shell
dolores@cluster demo$ seqspark MetaAnalysis meta.conf
```

##Appendices

### A.1. Setting-up Spark on a workstation

**0. preparation**

Suppose the workstation has six spare hard drives, 16 CPU cores, and 64 GB memory. The following is observed after formating and mounting the hard drives separately:

```shell
dolores@workstation: ~$ df -Th
Filesystem     Type      Size  Used Avail Use% Mounted on
/dev/sda1      ext4      459G  114G  345G  25% /mnt/hadoop/disk1
/dev/sdb1      ext4      459G  113G  346G  25% /mnt/hadoop/disk2
/dev/sdc1      ext4      459G  113G  347G  25% /mnt/hadoop/disk3
/dev/sdd1      ext4      459G  112G  347G  25% /mnt/hadoop/disk4
/dev/sde1      ext4      459G  114G  346G  25% /mnt/hadoop/disk5
/dev/sdf1      ext4      459G  113G  346G  25% /mnt/hadoop/disk6
...
```

Make sure you can ssh to the  `localhost` without typing the password. If you cannot, set it up as follows:

```shell
dolores@workstation: ~$ ssh-keygen
dolores@workstation: ~$ cat ~/.ssh/id_rsa.pub >> ~/.authorized_keys
dolores@workstation: ~$ chmod 600 ~/.authorized_keys
dolores@workstation: ~$ ssh localhost
dolores@workstation: ~$ exit
```

Download the suitable version of JDK for your system [here](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html), and then install it:

```shell
dolores@workstation: ~$ tar xf jdk-8u121-linux-x64.tar.gz
dolores@workstation: ~$ sudo cp -r jdk1.8.0_121 /opt/jdk1.8.0_121
```

**1. install and set up Hadoop**

Download and install:

```shell
dolores@workstation: ~$ wget http://mirror.olnevhost.net/pub/apache/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz
dolores@workstation: ~$ tar xf hadoop-2.6.5.tar.gz
dolores@workstation: ~$ sudo mv hadoop-2.6.5 /opt
```

Configure Hadoop:

```shell
dolores@workstation: ~$ cd /opt/hadoop-2.6.5/
dolores@workstation: hadoop-2.6.5$ mkdir data
dolores@workstation: cd etc/hadoop/
```

​	Edit the following files that are distributed with Hadoop as follows:

- `hadoop-env.sh`: 

   change the line `export JAVA_HOME=${JAVA_HOME}` to `export JAVA_HOME=/opt/jdk1.8.0_121`

- `core-site.xml`:

  ```xml
   <configuration>
       <property>
           <name>fs.defaultFS</name>
           <value>hdfs://localhost:9000</value>
       </property>
   </configuration>
  ```

- `hdfs-site.xml`:

  ```xml
   <configuration>
   	<property>
           <name>dfs.replication</name>
           <value>2</value>
       </property>
     	<property>
     		<name>dfs.namenode.name.dir</name>
     		<value>file:///opt/hadoop-2.6.5/data/namenode</value>
     	</property>
   	<property>
           <name>dfs.datanode.data.dir</name>     				<value>file:///mnt/hadoop/disk1,file:///mnt/hadoop/disk2,file:///mnt/hadoop/disk3,file:///mnt/hadoop/disk4,file:///mnt/hadoop/disk5,file:///mnt/hadoop/disk6</value>
       </property>
   	<property>
           <name>dfs.blocksize</name>
           <value>33554432</value>
       </property>
   </configuration>
  ```

Format HDFS and start the service:

```shell
dolores@workstation: hadoop$ cd ../..
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs namenode -format
dolores@workstation: hadoop-2.6.5$ ./sbin/start-dfs.sh
```

Check if it is working properly:

```shell
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs dfs -mkdir -p /user/dolores
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs dfs -put etc/hadoop/core-site.xml
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs dfs -cat core-site.xml
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs dfs -rm core-site.xml
```

Add the following to the file `~/.profile`  and re-login:

```shell
export PATH=/opt/hadoop-2.6.5/bin:$PATH
```

**2. install and configure Spark**

Download and install:

```shell
dolores@workstation: ~$ wget http://mirror.cogentco.com/pub/apache/spark/spark-2.1.0/spark-2.1.0-bin-without-hadoop.tgz
dolores@workstation: ~$ tar xf spark-2.1.0-bin-without-hadoop.tgz
dolores@workstation: ~$ sudo mv spark-2.1.0-bin-without-hadoop /opt/spark-2.1.0
```

Configure Spark:

```shell
dolores@workstation: ~$ cd /opt/spark-2.1.0/conf
dolores@workstation: conf$ cp spark-env.sh.template spark-env.sh
dolores@workstation: conf$ cp spark-defaults.conf.template spark-defaults.conf
dolores@workstation: conf$ cp log4j.properties.template log4j.properties
```

​	Edit the following files that are distributed with Spark as follows:

- `spark-env.sh`:

  ```shell
   export HADOOP_HOME=/opt/hadoop-2.6.5
   export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HADOOP_HOME}/lib/native
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   source ${HADOOP_CONF_DIR}/hadoop-env.sh
   export SPARK_DIST_CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath)
   export SPARK_WORKDER_MEMORY=52g
   export SPARK_WORKER_CORES=16
   export SPARK_MASTER_HOST=localhost
   export SPARK_LOCAL_IP=127.0.0.1
  ```

   Note: set `$SPARK_WORKER_MEMORY` and `$SPARK_WORKER_CORES` to a reasonable value for your server.

- `spark-default.conf`:

  ```json
   spark.master							spark://localhost:7077
   spark.executor.memory			 	 	 52g
   spark.driver.memory               	  	  8g
   spark.driver.maxResultSize			 	 6g
   spark.serializer						org.apache.spark.serializer.KryoSerializer
   spark.rdd.compress						true
   spark.kryoserializer.buffer.max		 1024m
   spark.sql.warehouse.dir				/user/dolores
  ```

   Note: set the memory usage options to reasonable values for your server.

- `log4j.properties`:

   add one line `log4j.logger.org.apache.spark=WARN`

Start Spark service:

```shell
dolores@workstation: conf$ cd /opt/spark-2.1.0
dolores@workstation: spark-2.1.0$ ./sbin/start-all.sh
```

Check if it is working properly:

```shell
dolores@workstation: spark-2.1.0$ ./bin/spark-shell
```

Add the following to the file `~/.profile` and re-login:

```shell
export PATH=/opt/spark-2.1.0/bin:$PATH
```

### A.2. Setting-up Spark on a cluster

**0. preparation**

Suppose the cluster to be set-up consist of 1 master server `node0` and 8 slave servers `node[1-8]`. Each slave server has 3 spare hard drives, 16 CPU cores, 64 GB memory. Format and mount the hard drives separately. The following is observed for each slave server:

```shell
dolores@node1: ~$ df -Th
Filesystem     Type      Size  Used Avail Use% Mounted on
/dev/sda1      ext4      459G  114G  345G  25% /mnt/hadoop/disk1
/dev/sdb1      ext4      459G  113G  346G  25% /mnt/hadoop/disk2
/dev/sdc1      ext4      459G  113G  347G  25% /mnt/hadoop/disk3
```

Make sure you can ssh to any of the nodes without typing the password.

Download the suitable version of JDK for your system [here](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html), and then install it for all nodes as follows:

```shell
dolores@node0: ~$ tar xf jdk-8u121-linux-x64.tar.gz
dolores@node0: ~$ sudo cp -r jdk1.8.0_121 /opt/jdk1.8.0_121
```

**1. install and set up Hadoop**

For all nodes, download and install:

```shell
dolores@node0: ~$ wget http://mirror.olnevhost.net/pub/apache/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz
dolores@node0: ~$ tar xf hadoop-2.6.5.tar.gz
dolores@node0: ~$ sudo mv hadoop-2.6.5 /opt
```

Configure Hadoop:

```shell
dolores@node0: ~$ cd /opt/hadoop-2.6.5/
dolores@node0: hadoop-2.6.5$ mkdir data
dolores@node0: cd etc/hadoop/
```

​       For all nodes, edit the following files that are distributed with Hadoop as follows: 

- `hadoop-env.sh`: 

  change the line `export JAVA_HOME=${JAVA_HOME}` to `export JAVA_HOME=/opt/jdk1.8.0_121`

- `core-site.xml`:

  ```xml
   <configuration>
       <property>
           <name>fs.defaultFS</name>
           <value>hdfs://node0:9000</value>
       </property>
     	<property>
   		<name>io.file.buffer.size</name>
   		<value>131072</value>
   	</property>
   </configuration>
  ```

   For `node0`, edit the following files:

- `hdfs-site.xml`:

  ```xml
   <configuration>
   	<property>
           <name>dfs.replication</name>
           <value>2</value>
       </property>
     	<property>
     		<name>dfs.namenode.name.dir</name>
     		<value>file:///opt/hadoop-2.6.5/data/namenode</value>
     	</property>
   	<property>
           <name>dfs.blocksize</name>
           <value>33554432</value>
       </property>
   </configuration>
  ```


- `slaves`:

  ```shell
  node1
  node2
  node3
  node4
  node5
  node6
  node7
  node8
  ```

  For `node[1-8]`, edit the following file:

- `hdfs-site.xml`

  ```xml
  <configuration>
  	<property>
          <name>dfs.datanode.data.dir</name>	
       <value>file:///mnt/hadoop/disk1,file:///mnt/hadoop/disk2,file:///mnt/hadoop/disk3</value>
      </property>
  </configuration>
  ```

Format HDFS and start the service:

```shell
dolores@node0: hadoop$ cd ../..
dolores@node0: hadoop-2.6.5$ ./bin/hdfs namenode -format
dolores@node0: hadoop-2.6.5$ ./sbin/start-dfs.sh
```

Check if it is working properly:

```shell
dolores@node0: hadoop-2.6.5$ ./bin/hdfs dfs -mkdir -p /user/dolores
dolores@node0: hadoop-2.6.5$ ./bin/hdfs dfs -put etc/hadoop/core-site.xml
dolores@node0: hadoop-2.6.5$ ./bin/hdfs dfs -cat core-site.xml
dolores@node0: hadoop-2.6.5$ ./bin/hdfs dfs -rm core-site.xml
```

Add the following to the file `~/.profile`  and re-login:

```shell
export PATH=/opt/hadoop-2.6.5/bin:$PATH
```

**2. install and configure Spark**

For all nodes, Download and install:

```shell
dolores@node0: ~$ wget http://mirror.cogentco.com/pub/apache/spark/spark-2.1.0/spark-2.1.0-bin-without-hadoop.tgz
dolores@node0: ~$ tar xf spark-2.1.0-bin-without-hadoop.tgz
dolores@node0: ~$ sudo mv spark-2.1.0-bin-without-hadoop /opt/spark-2.1.0
```

Configure Spark:

```shell
dolores@node0: ~$ cd /opt/spark-2.1.0/conf
dolores@node0: conf$ cp spark-env.sh.template spark-env.sh
dolores@node0: conf$ cp spark-defaults.conf.template spark-defaults.conf
dolores@node0: conf$ cp log4j.properties.template log4j.properties
```

​	For all nodes, edit the following files:

- `spark-env.sh`:

  ```shell
   export HADOOP_HOME=/opt/hadoop-2.6.5
   export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HADOOP_HOME}/lib/native
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   source ${HADOOP_CONF_DIR}/hadoop-env.sh
   export SPARK_DIST_CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath)
   export SPARK_WORKER_CORES=16
   export SPARK_WORKDER_MEMORY=52g
  ```

   Note: set the `$SPARK_WORKER_MEMORY` to a reasonable value for your server.

- `spark-default.conf`:

  ```json
   spark.master							spark://node0:7077
   spark.executor.memory			 	 	 52g
   spark.driver.memory               	  	  8g
   spark.driver.maxResultSize			 	 6g
   spark.serializer						org.apache.spark.serializer.KryoSerializer
   spark.rdd.compress						true
   spark.kryoserializer.buffer.max	      1024m
   spark.sql.warehouse.dir				 /user/dolores
  ```

   Note: set the memory usage options to reasonable values for your server.

  For `node0`, edit the following files:

- `log4j.properties`:

    add one line    `log4j.logger.org.apache.spark=WARN`

- `slaves`:

  ```shell
  node1
  node2
  node3
  node4
  node5
  node6
  node7
  node8
  ```

Start Spark service:

```shell
dolores@node0: conf$ cd /opt/spark-2.1.0
dolores@node0: spark-2.1.0$ ./sbin/start-all.sh
```

Check if it is working properly:

```shell
dolores@node0: spark-2.1.0$ ./bin/spark-shell
```

Add the following to the file `~/.profile` and re-login:

```shell
export PATH=/opt/spark-2.1.0/bin:$PATH
```
