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

  # TODO do we want to allow `match $x 10;` as a query without `isa`?
  Scenario Outline: '<type>' attributes can be matched by value
    Given graql define
      """
      define <attr> sub attribute, value <type>, owns ref @key;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert $n <value> isa <attr>, has ref 0;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $a <value>;
      """
    Then uniquely identify answer concepts
      | a         |
      | key:ref:0 |

    Examples:
      | attr        | type     | value      |
      | colour      | string   | "Green"    |
      | calories    | long     | 1761       |
      | grams       | double   | 9.6        |
      | gluten-free | boolean  | false      |
      | use-by-date | datetime | 2020-06-16 |


  # TODO do we want to allow $x 10 as a query without `isa`?
  Scenario Outline: when matching a '<type>' attribute by a value that doesn't exist, an empty answer is returned
    Given graql define
      """
      define <attr> sub attribute, value <type>, owns ref @key;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $a <value>;
      """
    Then answer size is: 0

    Examples:
      | attr        | type     | value      |
      | colour      | string   | "Green"    |
      | calories    | long     | 1761       |
      | grams       | double   | 9.6        |
      | gluten-free | boolean  | false      |
      | use-by-date | datetime | 2020-06-16 |

   # TODO we are now case sensitive apparently! Must include "isa attribute" else same error as above
  Scenario: 'contains' performs a case-insensitive match
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x "The Phantom of the Opera" isa name;
      $y "Pirates of the Caribbean" isa name;
      $z "Mr. Bean" isa name;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $x contains "Bean";
      """
    Then uniquely identify answer concepts
      | x                                   |
      | value:name:Pirates of the Caribbean |
      | value:name:Mr. Bean                 |
