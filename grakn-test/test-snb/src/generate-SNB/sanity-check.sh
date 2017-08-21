#!/bin/bash

DB_DIR="/opt/grakn/grakn-dist-0.13.0-SNAPSHOT/db/cassandra/data"
KEYSPACE="snb"

set -e

# size on disk
nodetool flush
SIZESTRING=$(du -hd 0 $DB_DIR)
SPLITSIZESTRING=($SIZESTRING)
echo "The size on disk is: ${SPLITSIZESTRING[0]}"

# number of concepts
CONCEPTS=$(graql.sh -k $KEYSPACE -e "compute count;")
echo "The number of concepts is: $CONCEPTS"

# duplicates
DISTINCT=$(graql.sh -k $KEYSPACE -e "match \$x isa gender; distinct; aggregate count;")
ALL=$(graql.sh -k $KEYSPACE -e "match \$x isa gender; aggregate count;")
if [ "$DISTINCT" == "$ALL" ]; then
	echo "No duplicates."
else
	echo "There were duplicates in the graph."
	exit 1
fi
