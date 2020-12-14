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


  # TODO this fails because we use relation:someplayer as a `label` property to create an iterator over. This label does
  # TODO not exist as a type!
  Scenario: duplicate role players are retrieved singly when queried doubly
    Given graql define
      """
      define
      some-entity sub entity, plays symmetric:someplayer, owns ref @key;
      symmetric sub relation, relates someplayer, owns ref @key;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert $x isa some-entity, has ref 0; (someplayer: $x, someplayer: $x) isa symmetric, has ref 1;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $r (someplayer: $x) isa relation;
      """
    And concept identifiers are
      |      | check | value |
      | REF0 | key   | ref:0 |
      | REF1 | key   | ref:1 |
    Then uniquely identify answer concepts
      | x    | r    |
      | REF0 | REF1 |

  # TODO traversal bug - requires traversal language, very nondeterministically failing
  # TODO note: can reproduce bug sometimes with only the query: `match (sender: $a, recipient: $b) isa gift-delivery`
  Scenario: matching a chain of relations only returns answers if there is a chain of the required length
    Given graql define
      """
      define

      gift-delivery sub relation,
        relates sender,
        relates recipient;

      person plays gift-delivery:sender,
        plays gift-delivery:recipient;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x1 isa person, has name "Soroush", has ref 0;
      $x2a isa person, has name "Martha", has ref 1;
      $x2b isa person, has name "Patricia", has ref 2;
      $x2c isa person, has name "Lily", has ref 3;

      (sender: $x1, recipient: $x2a) isa gift-delivery;
      (sender: $x1, recipient: $x2b) isa gift-delivery;
      (sender: $x1, recipient: $x2c) isa gift-delivery;
      (sender: $x2a, recipient: $x2b) isa gift-delivery;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match
        (sender: $a, recipient: $b) isa gift-delivery;
        (sender: $b, recipient: $c) isa gift-delivery;
      """
    When concept identifiers are
      |     | check | value |
      | SOR | key   | ref:0 |
      | MAR | key   | ref:1 |
      | PAT | key   | ref:2 |
    Then uniquely identify answer concepts
      | a   | b   | c   |
      | SOR | MAR | PAT |
    When get answers of graql query
      """
      match
        (sender: $a, recipient: $b) isa gift-delivery;
        (sender: $b, recipient: $c) isa gift-delivery;
        (sender: $c, recipient: $d) isa gift-delivery;
      """
    Then answer size is: 0


  # TODO traversal bug, no answers
  Scenario: an attribute variable used in both '=' and '>=' predicates is correctly resolved
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
      match
        $x has age = $z;
        $z >= 17;
        $z isa age;
      get $x;
      """
    And concept identifiers are
      |     | check | value |
      | DON | key   | ref:1 |
      | RAL | key   | ref:2 |
    Then uniquely identify answer concepts
      | x   |
      | DON |
      | RAL |
