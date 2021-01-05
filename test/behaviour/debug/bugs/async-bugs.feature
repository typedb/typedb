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

  Scenario: 'sub' can be used to retrieve all instances of types that are subtypes of a given type
    Given graql define
      """
      define

      child sub person;
      worker sub person;
      retired-person sub person;
      construction-worker sub worker;
      bricklayer sub construction-worker;
      crane-driver sub construction-worker;
      telecoms-worker sub worker;
      mobile-network-researcher sub telecoms-worker;
      smartphone-designer sub telecoms-worker;
      telecoms-business-strategist sub telecoms-worker;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $a isa child, has name "Alfred", has ref 0;
      $b isa retired-person, has name "Barbara", has ref 1;
      $c isa bricklayer, has name "Charles", has ref 2;
      $d isa crane-driver, has name "Debbie", has ref 3;
      $e isa mobile-network-researcher, has name "Edmund", has ref 4;
      $f isa telecoms-business-strategist, has name "Felicia", has ref 5;
      $g isa worker, has name "Gary", has ref 6;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match
        $x isa $type;
        $type sub worker;
      """
    # Alfred and Barbara are not retrieved, as they aren't subtypes of worker
    Then uniquely identify answer concepts
      | x         | type                               |
      | key:ref:2 | label:bricklayer                   |
      | key:ref:2 | label:construction-worker          |
      | key:ref:2 | label:worker                       |
      | key:ref:3 | label:crane-driver                 |
      | key:ref:3 | label:construction-worker          |
      | key:ref:3 | label:worker                       |
      | key:ref:4 | label:mobile-network-researcher    |
      | key:ref:4 | label:telecoms-worker              |
      | key:ref:4 | label:worker                       |
      | key:ref:5 | label:telecoms-business-strategist |
      | key:ref:5 | label:telecoms-worker              |
      | key:ref:5 | label:worker                       |
      | key:ref:6 | label:worker                       |

  Scenario: 'has $attr != $x' matches owners of any instance '$y' of '$attr' where '$y != $x'
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x isa person, has name "Susie", has age 16, has ref 0;
      $y isa person, has name "Donald", has age 25, has ref 1;
      $z isa person, has name "Ralph", has age 18, has ref 2;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $x has age != 18;
      """
    Then uniquely identify answer concepts
      | x         |
      | key:ref:0 |
      | key:ref:1 |

  Scenario: all instances and their types can be retrieved
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
      match $x isa entity;
      """
    Given answer size is: 2
    Given get answers of graql query
      """
      match $r isa relation;
      """
    Given answer size is: 1
    Given get answers of graql query
      """
      match $x isa attribute;
      """
    Given answer size is: 5
    When get answers of graql query
      """
      match $x isa $type;
      """
    # 2 entities x 3 types {person,entity,thing}
    # 1 relation x 3 types {friendship,relation,thing}
    # 5 attributes x 3 types {ref/name,attribute,thing}
    Then answer size is: 24