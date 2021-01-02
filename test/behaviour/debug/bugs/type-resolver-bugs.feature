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

  Scenario: when matching a roleplayer in a relation that can't actually play that role, an empty result is returned
    When get answers of graql query
      """
      match
      $x isa company;
      ($x) isa friendship;
      """
    Then answer size is: 0

  Scenario: Relations can be queried with pairings of relation and role types that are not directly related to each other
    Given graql define
      """
      define
      person plays marriage:spouse, plays hetero-marriage:husband, plays hetero-marriage:wife;
      marriage sub relation, relates spouse;
      hetero-marriage sub marriage, relates husband as spouse, relates wife as spouse;
      civil-marriage sub marriage;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $a isa person, has ref 1;
      $b isa person, has ref 2;
      (wife: $a, husband: $b) isa hetero-marriage;
      (spouse: $a, spouse: $b) isa civil-marriage;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $m (wife: $x, husband: $y) isa hetero-marriage;
      """
    Then answer size is: 1
    Then graql match; throws exception
      """
      match $m (wife: $x, husband: $y) isa civil-marriage;
      """
    When get answers of graql query
      """
      match $m (wife: $x, husband: $y) isa marriage;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match $m (wife: $x, husband: $y) isa relation;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match $m (spouse: $x, spouse: $y) isa hetero-marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match $m (spouse: $x, spouse: $y) isa civil-marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match $m (spouse: $x, spouse: $y) isa marriage;
      """
    Then answer size is: 4
    When get answers of graql query
      """
      match $m (spouse: $x, spouse: $y) isa relation;
      """
    Then answer size is: 4
    When get answers of graql query
      """
      match $m (role: $x, role: $y) isa hetero-marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match $m (role: $x, role: $y) isa civil-marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match $m (role: $x, role: $y) isa marriage;
      """
    Then answer size is: 4
    When get answers of graql query
      """
      match $m (role: $x, role: $y) isa relation;
      """
    Then answer size is: 4

  Scenario: 'has' can match instances that have themselves
    Given graql define
      """
      define
     unit sub attribute, value string, owns unit, owns ref;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x "meter" isa unit, has $x, has ref 0;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $x has $x;
      """
    Then uniquely identify answer concepts
      | x         |
      | key:ref:0 |

  Scenario: when matching by an attribute ownership, if the owner can't actually own it, an empty result is returned
    When get answers of graql query
      """
      match $x isa company, has age $n;
      """
    Then answer size is: 0

  Scenario: value comparisons can be performed between a 'double' and a 'long'
    Given graql define
      """
      define
      house-number sub attribute, value long;
      length sub attribute, value double;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x 1 isa house-number;
      $y 2.0 isa length;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match
        $x isa house-number;
        $x = 1.0;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match
        $x isa length;
        $x = 2;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match
        $x isa house-number;
        $x 1.0;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match
        $x isa length;
        $x 2;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match
        $x isa attribute;
        $x >= 1;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match
        $x isa attribute;
        $x < 2.0;
      """
    Then answer size is: 1

  Scenario: when the answers of a value comparison include both a 'double' and a 'long', both answers are returned
    Given graql define
      """
      define
      length sub attribute, value double;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $a 24 isa age;
      $b 19 isa age;
      $c 20.9 isa length;
      $d 19.9 isa length;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match
        $x isa attribute;
        $x > 20;
      """
    Then uniquely identify answer concepts
      | x                 |
      | value:age:24      |
      | value:length:20.9 |

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
