#!/usr/bin/env bash

mvn test --projects "$1" -Dtest=**/*$2* -DfailIfNoTests=false jacoco:report
bash <(curl -s https://codecov.io/bash)
