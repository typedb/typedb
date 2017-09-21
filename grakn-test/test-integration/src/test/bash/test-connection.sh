#!/usr/bin/env bash

source env.sh

graql console -e 'match $x; get;'  # Sanity check query. I.e. is everything working?
