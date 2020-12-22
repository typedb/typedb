#
# Copyright (C) 2020 Grakn Labs
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
Feature: Graql Match Query

  Background: Open connection and create a simple extensible schema
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: grakn
    Given connection open schema session for database: grakn
    Given session opens transaction of type: write
    Given the integrity is validated
    Given graql define
      """
      define
      person sub entity,
        plays friendship:friend,
        plays employment:employee,
        owns name,
        owns age,
        owns ref @key;
      company sub entity,
        plays employment:employer,
        owns name,
        owns ref @key;
      friendship sub relation,
        relates friend,
        owns ref @key;
      employment sub relation,
        relates employee,
        relates employer,
        owns ref @key;
      name sub attribute, value string;
      age sub attribute, value long;
      ref sub attribute, value long;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: write


  Scenario: 'isa' gets any thing for any type
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $_ isa person, has ref 0;
      $_ isa person, has ref 1;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $x isa $y;
      """
    Then uniquely identify answer concepts
      | x           | y               |
      | key:ref:0   | label:person    |
#      | key:ref:0   | label:entity    |
#      | key:ref:0   | label:thing     |
      | key:ref:1   | label:person    |
#      | key:ref:1   | label:entity    |
#      | key:ref:1   | label:thing     |
      | value:ref:0 | label:ref       |
#      | value:ref:0 | label:attribute |
#      | value:ref:0 | label:thing     |
      | value:ref:1 | label:ref       |
#      | value:ref:1 | label:attribute |
#      | value:ref:1 | label:thing     |
