#!/usr/bin/env bash

# Rewire log directory and make it world-writeable
rm -rf /opt/grakn/core/logs
mkdir -m 777 -p /var/log/grakn/server/
ln -s /var/log/grakn/server/ /opt/grakn/core/logs
