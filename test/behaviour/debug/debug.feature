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
Feature: Graql Match Clause

  Background: Create a simple schema that is extensible for each scenario
    Given connection has been opened
    Given connection delete all keyspaces
    Given connection open sessions for keyspaces:
      | test_match |
    Given transaction is initialised
    Given the integrity is validated

  Scenario: Disjunctions return the union of composing query statements

  Scenario: a relation is matchable from role players without specifying relation type
    Given graql define
      """
      define
      person sub entity,
        plays employee,
        key ref;
      company sub entity,
        plays employer,
        key ref;
      employment sub relation,
        relates employee,
        relates employer,
        key ref;
      ref sub attribute, datatype long;
      """
    Given the integrity is validated

    When graql insert
      """
      insert
      $x isa person, has ref 0;
      $y isa company, has ref 1;
      $r (employee: $x, employer: $y) isa employment,
         has ref 2;
      """
    When the integrity is validated

    Then get answers of graql query
      """
      match $x isa person; $r (employee: $x) isa relation; get;
      """
    Then answer concepts all have key: ref
    Then answer keys are
      | x    | r    |
      | 0    | 2    |

    Then get answers of graql query
      | match $y isa company; $r (employer: $y) isa relation; get; |
    Then answer concepts all have key: ref
    Then answer keys are
      | y    | r    |
      | 1    | 2    |


  Scenario: retrieve all combinations of players in a relation
    Given graql define
      """
      define
      person sub entity,
        plays employee,
        key ref;
      company sub entity,
        plays employer,
        key ref;
      employment sub relation,
        relates employee,
        relates employer,
        key ref;
      ref sub attribute, datatype long;
      """
    Given the integrity is validated

    When graql insert
      """
      insert $p isa person, has ref 0;
      $c isa company, has ref 1;
      $c2 isa company, has ref 2;
      $r (employee: $p, employer: $c, employer: $c2) isa employment, has ref 3;
      """
    When the integrity is validated

    Then get answers of graql query
      """
      match $r ($x, $y) isa employment; get; 
      """
    Then answer concepts all have key: ref
    Then answer keys are
      | x    | y    | r    |
      | 0    | 1    | 3    |
      | 1    | 0    | 3    |
      | 0    | 2    | 3    |
      | 2    | 0    | 3    |
      | 1    | 2    | 3    |
      | 2    | 1    | 3    |


  Scenario: subtype hierarchy satisfies transitive sub assertions
    Given graql define
      """
      define
      sub1 sub entity;
      sub2 sub sub1;
      sub3 sub sub1;
      sub4 sub sub2;
      sub5 sub sub4;
      sub6 sub sub5;
      """
    Given the integrity is validated

    When get answers of graql query
      """
      match $x sub $y; $y sub $z; get;
      """
    Then each answer satisfies
      """
      match $x sub $z; $x id <answer.x.id>; $z id <answer.z.id>; get;
      """

  Scenario: duplicate role players are retrieved singly when queried doubly
    Given graql define
      """
      define
        some-entity sub entity, plays player, key ref;
        symmetric sub relation, relates player, key ref;
        ref sub attribute, datatype long;
      """
    Given the integrity is validated

    Given graql insert
      """
      insert $x isa some-entity, has ref 0; (player: $x, player: $x) isa symmetric, has ref 1;
      """
    Given the integrity is validated

    When get answers of graql query
      """
      match $r (player: $x, player: $x) isa relation; get;
      """
    Then answer concepts all have key: ref
    Then answer keys are
      | x    |  r    |
      | 0    |  1    |

  Scenario: duplicate role players are retrieved singly when queried singly
    Given graql define
      """
      define
        some-entity sub entity, plays player, key ref;
        symmetric sub relation, relates player, key ref;
        ref sub attribute, datatype long;
      """
    Given the integrity is validated

    Given graql insert
      """
      insert $x isa some-entity, has ref 0; (player: $x, player: $x) isa symmetric, has ref 1;
      """
    Given the integrity is validated

    When get answers of graql query
      """
      match $r (player: $x) isa relation; get;
      """
    Then answer concepts all have key: ref
    Then answer keys are
      | x    |  r    |
      | 0    |  1    |
