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

  Scenario: 'sub' can be used to match the specified type and all its supertypes, including indirect supertypes
    Given graql define
      """
      define
      writer sub person;
      scifi-writer sub writer;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match writer sub $x;
      """
    Then uniquely identify answer concepts
      | x            |
      | label:writer |
      | label:person |
      | label:entity |
      | label:thing  |

  Scenario: duplicate role players are retrieved singly when queried doubly
    Given graql define
      """
      define
      some-entity sub entity, plays symmetric:player, owns ref @key;
      symmetric sub relation, relates player, owns ref @key;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert $x isa some-entity, has ref 0; (player: $x, player: $x) isa symmetric, has ref 1;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match $r (player: $x, player: $x) isa relation;
      """
    Then uniquely identify answer concepts
      | x         | r         |
      | key:ref:0 | key:ref:1 |

  Scenario: relations between distinct concepts are not retrieved when matching concepts that relate to themselves
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x isa person, has ref 1;
      $y isa person, has ref 2;
      (friend: $x, friend: $y) isa friendship, has ref 0;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match (friend: $x, friend: $x) isa friendship;
      """
    Then answer size is: 0

  Scenario: Relations can be queried with pairings of relation and role types that are not directly related to each other
    Given graql define
      """
      define
      person plays hetero-marriage:husband, plays hetero-marriage:wife;
      marriage sub relation, relates spouse;
      hetero-marriage sub marriage, relates husband as spouse, relates wife as spouse;
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
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match (wife: $x, husband: $y) isa relation;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match (wife: $x, husband: $y) isa marriage;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match (wife: $x, husband: $y) isa hetero-marriage;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match (spouse: $x, spouse: $y) isa hetero-marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (spouse: $x, spouse: $y) isa marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (spouse: $x, spouse: $y) isa relation;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (role: $x, role: $y) isa hetero-marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (role: $x, role: $y) isa marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (role: $x, role: $y) isa relation;
      """
    Then answer size is: 2

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
    Then uniquely identify answer concepts
      | a         | b         | c         |
      | key:ref:0 | key:ref:1 | key:ref:2 |
    When get answers of graql query
      """
      match
        (sender: $a, recipient: $b) isa gift-delivery;
        (sender: $b, recipient: $c) isa gift-delivery;
        (sender: $c, recipient: $d) isa gift-delivery;
      """
    Then answer size is: 0

  Scenario: when multiple relation instances exist with the same roleplayer, matching that player returns just 1 answer
    Given graql define
      """
      define
      residency sub relation,
        relates resident,
        owns ref @key;
      person plays residency:resident;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x isa person, has ref 0;
      $e (employee: $x) isa employment, has ref 1;
      $f (friend: $x) isa friendship, has ref 2;
      $r (resident: $x) isa residency, has ref 3;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    Given get answers of graql query
      """
      match $r isa relation;
      """
    Given uniquely identify answer concepts
      | r         |
      | key:ref:1 |
      | key:ref:2 |
      | key:ref:3 |
    When get answers of graql query
      """
      match ($x) isa relation;
      """
    Then uniquely identify answer concepts
      | x         |
      | key:ref:0 |
    When get answers of graql query
      """
      match ($x);
      """
    Then uniquely identify answer concepts
      | x         |
      | key:ref:0 |