#!/usr/bin/env bash

# Rewire log directory and make it world-writeable
rm -rf /opt/grakn/core/console/logs
mkdir -m 777 -p /var/log/grakn/console/
ln -s /var/log/grakn/console/ /opt/grakn/core/console/logs

