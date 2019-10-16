# README

SEQSpark was developed to analyze large-scale genotype data that consists of many samples and a large number of variants generated from Whole-Genome-Sequencing (WGS) or Exome-Sequencing (ES) project. SEQSpark can also analyze imputed genotype data. In this manual, is described SEQSpark's functionalities in performing anotation, data quality control and association testing.

For documentation, please visit https://statgenetics.github.io/seqspark/.


## Docker image for development

### Get the image
You can either build the image yourself from the `Dockerfile` or pull the image we built from Dockerhub.

- **Option 1:** Build from the `Dockerfile`:
```bash
docker build -t seqspark .
```
- **Option 2:** Pull from Dockerhub:
```bash
docker pull zhangdbio/seqspark:test
docker tag zhangdbio/seqspark:test seqspark:latest
```

### Download the database files for annotation
The docker image doesn't include any database files, so you need to download them to your host machine and then attach the `volume` to the container.

```bash
mkdir -p db
wget seqspark.statgen.us/refFlat_table -P db
wget seqspark.statgen.us/refGene_seq -P db
wget seqspark.statgen.us/dbSNP-138.vcf.bz2 -P db
```

### Run docker
The database directory must be mounted to `/opt/seqspark/ref` and the current directory can be mounted to the home directory `/home/seqspark`.

```bash
docker run -v ${PWD}/db:/opt/seqspark/ref -v ${PWD}:/home/seqspark seqspark <your_project.conf>
```
**Notes**:
- Replace `<your_project.conf>` with your own configuration file.
- The database directory on host doesn't have to be in the current directory. If that is the case, replace `${PWD}/db` with the actual path of it.

### Login into the container
If you prefer to playing with the demo interactively:
```bash
docker run -it --entrypoint=sh -v ${PWD}/db:/opt/seqspark/ref -v ${PWD}:/home/seqspark seqspark
```
