#!/usr/bin/env bash

set -e

cd docs

rake dependencies
rake test