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
Feature: Invalid Casting Test

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

  # TODO invalid casting from Identifier.Variable.Name to Identifier.Scoped
  Scenario: relations are matchable from roleplayers without specifying any roles
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
    When get answers of graql query
      """
      match $x isa person; $r ($x) isa relation;
      """
    When concept identifiers are
      |      | check | value |
      | REF0 | key   | ref:0 |
      | REF2 | key   | ref:2 |
    Then uniquely identify answer concepts
      | x    | r    |
      | REF0 | REF2 |

  # TODO invalid casting from Identifier.Variable.Name to Identifier.Scoped
  Scenario: all combinations of players in a relation can be retrieved
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    When graql insert
      """
      insert $p isa person, has ref 0;
      $c isa company, has ref 1;
      $c2 isa company, has ref 2;
      $r (employee: $p, employer: $c, employer: $c2) isa employment, has ref 3;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    Then get answers of graql query
      """
      match $r ($x, $y) isa employment;
      """
    And concept identifiers are
      |      | check | value |
      | REF0 | key   | ref:0 |
      | REF1 | key   | ref:1 |
      | REF2 | key   | ref:2 |
      | REF3 | key   | ref:3 |
    Then uniquely identify answer concepts
      | x    | y    | r    |
      | REF0 | REF1 | REF3 |
      | REF1 | REF0 | REF3 |
      | REF0 | REF2 | REF3 |
      | REF2 | REF0 | REF3 |
      | REF1 | REF2 | REF3 |
      | REF2 | REF1 | REF3 |

  # TODO invalid casting from Identifier.Variable.Name to Identifier.Scoped
  Scenario: all relations and their types can be retrieved
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
    """
          insert
          $x isa person, has name "Bertie", has ref 0;
          $y isa person, has name "Angelina", has ref 1;
          $r (friend: $x, friend: $y) isa friendship, has ref 2;
          """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    Given get answers of graql query
    """
          match $r isa relation;
          """
    Given answer size is: 1
    Given get answers of graql query
    """
          match ($x, $y) isa relation;
          """
        # 2 permutations of the roleplayers
    Given answer size is: 2
    When get answers of graql query
    """
          match ($x, $y) isa $type;
          """
        # 2 permutations x 3 types {friendship,relation,thing}
    Then answer size is: 6


  # TODO invalid casting from Identifier.Variable.Name to Identifier.Scoped
  Scenario: all relations and their types can be retrieved
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x isa person, has name "Bertie", has ref 0;
      $y isa person, has name "Angelina", has ref 1;
      $r (friend: $x, friend: $y) isa friendship, has ref 2;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    Given get answers of graql query
      """
      match $r isa relation;
      """
    Given answer size is: 1
    Given get answers of graql query
      """
      match ($x, $y) isa relation;
      """
    # 2 permutations of the roleplayers
    Given answer size is: 2
    When get answers of graql query
      """
      match ($x, $y) isa $type;
      """
    # 2 permutations x 3 types {friendship,relation,thing}
    Then answer size is: 6
