# Description
The `test-snb` project perform tests with the [SNB data](ldbcouncil.org/developer/snb). In principle, there are two major steps involved:
1. Generating an initial dataset and loading them into Grakn
2. Performing a verification if the data has been inserted correctly

## Prerequisites
### Hadoop
1. Download Hadoop (http://hadoop.apache.org/releases.html) and extract it.
2. Add an environment variable pointing to where it was extracted: `export HADOOP_HOME=/path/to/hadoop`

### SNB Data generator
1. Clone the [SNB LDDBC Datagen](https://github.com/ldbc/ldbc_snb_datagen) repository.
2. Add an environment variable pointing to where it was extracted: `export LDBC_SNB_DATAGEN_HOME=/path/to/snb-ldbc-datagen`
3. Compile and generate a fat-JAR using Maven: `cd /path/to/snb-ldbc-datagen && mvn clean compile assembly:single`

## Running The Validation
1. Load the validation data set and perform data verification: `cd $GRAKN_HOME && ./scripts/jenkins.sh test-snb`
2. Load the `SF1` dataset: `cd $GRAKN_HOME && ./scripts/load.sh test-snb gen SF1`
3. Perform validation only: `cd $GRAKN_HOME && ./scripts/validate.sh test-snb`