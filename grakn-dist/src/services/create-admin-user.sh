#!/bin/bash

###
# #%L
# grakn-dist
# %%
# Copyright (C) 2016 - 2018 Grakn Labs Ltd
# %%
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# #L%
###

# Defaults
DEFAULT_ENGINE_HOST="http://localhost:4567"

# Check for curl
if ! hash curl 2>/dev/null; then
  echo "curl doesn't exist"
  exit 1
fi

read -p "Enter Grakn host (${DEFAULT_ENGINE_HOST}): " ENGINE_HOST
read -p "Enter username: " USER_NAME
read -s -p "Enter password: " USER_PASSWORD
echo ""

if [[ -z "${ENGINE_HOST}" ]]; then
  ENGINE_HOST="${DEFAULT_ENGINE_HOST}"
fi

if [[ -z "${USER_NAME}" || -z "${USER_PASSWORD}" ]]; then
  echo "username/password cannot be empty."
  echo ""
  exit 1
else
  API_RESPONSE=$(curl -s -H 'Content-Type: application/json' -X POST "${ENGINE_HOST}/user/one" --data "{\"user-name\": \"${USER_NAME}\", \"user-password\": \"${USER_PASSWORD}\", \"user-is-admin\": true}")
  echo ""
fi

if [[ "${API_RESPONSE}" == "false" ]]; then
  echo "User creation failed - already exists?"
elif [[ "${API_RESPONSE}" == "true" ]]; then
  echo "User ${USER_NAME} has been created."
  echo "Remember to enable authentication in the config file and restart Grakn Engine."
else
  echo "API ERROR: ${API_RESPONSE}"
fi
