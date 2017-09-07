#!/usr/bin/env bash

source env.sh

download-snb.sh
load-SNB.sh arch validate
measure-size.sh