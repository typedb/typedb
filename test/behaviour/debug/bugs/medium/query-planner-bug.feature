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


  ##################
  # SCHEMA QUERIES #
  ##################

  # TODO fails with planning exception
  Scenario: 'owns' can match types that can own themselves
    Given graql define
      """
      define
      unit sub attribute, value string, owns unit;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $x owns $x;
      """
    And concept identifiers are
      |      | check | value  |
      | UNIT | label | unit   |
    Then uniquely identify answer concepts
      | x    |
      | UNIT |


  # TODO query planning error
  Scenario: when things own attributes of different types but the same value, they match by equality
    Given graql define
      """
      define
      start-date sub attribute, value datetime;
      graduation-date sub attribute, value datetime;
      person owns graduation-date;
      employment owns start-date;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x isa person, has name "James", has ref 0, has graduation-date 2009-07-16;
      $r (employee: $x) isa employment, has start-date 2009-07-16, has ref 1;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    Then get answers of graql query
      """
      match
        $x isa person, has graduation-date $date;
        $r (employee: $x) isa employment, has start-date = $date;
      """
    Then answer size is: 1