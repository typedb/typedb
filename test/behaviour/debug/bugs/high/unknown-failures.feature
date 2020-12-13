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

#noinspection CucumberUndefinedStep
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


  # TODO unknown reason for failure
  Scenario: a relation is matchable from role players without specifying relation type
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x isa person, has ref 0;
      $y isa company, has ref 1;
      $r (employee: $x, employer: $y) isa employment,
         has ref 2;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    Then get answers of graql query
      """
      match $x isa person; $r (employee: $x) isa relation;
      """
    And concept identifiers are
      |      | check | value |
      | REF0 | key   | ref:0 |
      | REF1 | key   | ref:1 |
      | REF2 | key   | ref:2 |
    Then uniquely identify answer concepts
      | x    | r    |
      | REF0 | REF2 |
    When get answers of graql query
      """
      match $y isa company; $r (employer: $y) isa relation;
      """
    Then uniquely identify answer concepts
      | y    | r    |
      | REF1 | REF2 |
