---
title: This will be used as the title-tag of the page head
---


# Reference configurations

## 1. root

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

## 2. input

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

## 3. qualityControl

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

## 4. association

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

### 4.1. trait

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

### 4.2. method

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

## 5. meta-analysis

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
dolores@cluster demo$ seqspark meta.conf
```

