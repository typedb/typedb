#!/usr/bin/env bash

# force script to exit on failed command
set -e

graql.sh -e 'match $x;'  # Sanity check query. I.e. is everything working?
