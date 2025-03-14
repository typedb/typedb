# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  Background: Open connection and create a simple extensible schema
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true
    Given connection has 0 databases
    Given connection create database: typedb
    Given connection open schema transaction for database: typedb

    Given typeql schema query
      """
      define
      entity person,
        plays friendship:friend,
        plays employment:employee,
        owns name @key,
        owns email @card(0..);
      entity company,
        plays employment:employer;
      relation friendship,
        relates friend @card(0..),
        owns ref @key;
      relation employment,
        relates employee,
        relates employer,
        owns ref @key;
      attribute name @independent, value string;
      attribute email @independent, value string;
      attribute ref @independent, value integer;
      """
    Given transaction commits

    Given connection open write transaction for database: typedb

#  Even when a $role variable matches multiple roles (will always match 'role' unless constrained)
#  We only delete role player edges until the 'match' is no longer satisfied.
#
#  **Sometimes this means multiple repeated role players will be unassigned **
#
#  For example
#
#  // concrete instance:  $r isa directed-by (production: $x, production: $x, production: $x, director: $y);
#  match $r isa directed-by ($role1: $x, director: $y); $type sub work;
#  delete links ($role1: $x) of $r;
#
#  First, we will match '$role1' = ROLE meta role. Using this answer we will remove a single $x from $r via the 'production'.
#  Next, we will match '$role1' = WORK role, and we delete another 'production' player. This repeats again for $role='production'.
  Scenario: when deleting repeated role players with a single variable role, both repetitions are removed
    Given transaction closes
    Given connection open schema transaction for database: typedb
    Given typeql schema query
      """
      define
      relation ship-crew, relates captain, relates navigator, relates chef, owns ref @key;
      person plays ship-crew:captain, plays ship-crew:navigator, plays ship-crew:chef;
      """
    Given transaction commits

    Given connection open write transaction for database: typedb
    Given typeql write query
      """
      insert
      $x isa person, has name "Cook";
      $y isa person, has name "Joshua";
      $r isa ship-crew (captain: $x, chef: $y, chef: $y), has ref 0;
      """
    Given transaction commits

    When connection open write transaction for database: typedb
    When get answers of typeql read query
      """
      match $rel isa ship-crew (chef: $p);
      """
    Then uniquely identify answer concepts
      | rel       | p               |
      | key:ref:0 | key:name:Joshua |
    When get answers of typeql read query
      """
      match $r isa ship-crew ($role1: $x, captain: $y);
      """
    Then answer size is: 2

    When typeql write query
      """
      match
        $r isa ship-crew ($role1: $x, captain: $y);
      delete
        links ($role1: $x) of $r;
      """
    Then transaction commits

    When connection open read transaction for database: typedb
    When get answers of typeql read query
      """
      match $rel isa ship-crew (chef: $p);
      """
    Then answer size is: 0
