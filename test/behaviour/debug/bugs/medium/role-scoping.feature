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
Feature: Role Scoping

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

  # TODO fails to find answers because "employee" is a scoped label without a scope, so it never gets matched by a
  # TODO vertex that does have a scoped label
  Scenario: 'relates' matches relation types where the specified role can be played
    When get answers of graql query
      """
      match $x relates employee;
      """
    And concept identifiers are
      |     | check | value      |
      | EMP | label | employment |
    Then uniquely identify answer concepts
      | x   |
      | EMP |



  # TODO fails to find answers because "friend" is a scoped label without a scope, so it never gets matched by a
  # TODO vertex that does have a scoped label
  Scenario: 'relates' without 'as' does not match relation types that override the specified roleplayer
    Given graql define
      """
      define
      close-friendship sub friendship, relates close-friend as friend;
      friendly-person sub entity, plays close-friendship:close-friend;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $x relates friend;
      """
    And concept identifiers are
      |     | check | value      |
      | FRE | label | friendship |
    Then uniquely identify answer concepts
      | x   |
      | FRE |
