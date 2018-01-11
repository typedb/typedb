# Description
The `test-snb` project perform tests with the [SNB data](ldbcouncil.org/developer/snb). In principle, there are three major steps involved:
1. Generating an initial dataset (with the help of the SNB LDBC Datagen)
2. Loading the schema and the generated data into Grakn (with the help of `graql migrate`)
3. Validating if the data has been inserted correctly (done with the `GraknDB` client implementation located under `grakn-test/test-snb/src/main/java/ai/grakn/GraknDB.java`)

## Prerequisites
### Hadoop
1. Download Hadoop (http://hadoop.apache.org/releases.html) and extract it.
2. Add an environment variable pointing to where it was extracted: `export HADOOP_HOME=/path/to/hadoop`

### SNB Data generator
1. Clone the [SNB LDBC Datagen](https://github.com/ldbc/ldbc_snb_datagen) repository.
2. Add an environment variable pointing to where it was extracted: `export LDBC_SNB_DATAGEN_HOME=/path/to/snb-ldbc-datagen`
3. Compile and generate a fat-JAR using Maven: `cd /path/to/snb-ldbc-datagen && mvn clean compile assembly:single`

## Running The Validation
The following steps outlines generating, loading, and validating data with the "validation dataset". Currently, it is not possible to validate other datasets such as the `SF*` dataset.
1. If you want to do all the steps (generate an initial dataset, loading the data into Grakn, and performing the validation)at once: `cd $GRAKN_HOME && ./scripts/jenkins.sh test-snb`
2. If you want to load the validation dataset only: `cd $GRAKN_HOME && ./scripts/load.sh test-snb`
3. If you want to perform only the validation step on the validation dataset: `cd $GRAKN_HOME && ./scripts/validate.sh test-snb`

The following steps outlines generating the `SF*` dataset:
1. `cd $GRAKN_HOME && ./scripts/load.sh test-snb gen SF1`