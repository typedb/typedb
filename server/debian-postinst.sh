#!/usr/bin/env bash

# Make Grakn data and Cassandra config world-writeable
chmod a+rwx /opt/grakn/core/server/services/cassandra/cassandra.yaml

# Rewire log directory and make it world-writeable
rm -rf /opt/grakn/core/server/logs
mkdir -m 777 -p /var/log/grakn/server/
ln -s /var/log/grakn/server/ /opt/grakn/core/server/logs

rm -rf /opt/grakn/core/server/db/
mkdir -m 777 -p /var/lib/grakn/db/cassandra /var/lib/grakn/db/queue
ln -s /var/lib/grakn/db/ /opt/grakn/core/server/db
