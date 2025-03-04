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
        plays parentship:parent,
        plays parentship:child,
        owns name,
        owns ref @key;
      relation friendship,
        relates friend,
        owns ref @key;
      relation parentship,
        relates parent,
        relates child,
        owns ref;
      attribute name value string;
      attribute ref @independent, value integer;
      """
    Given transaction commits
    Given connection open write transaction for database: typedb

  Scenario: Update queries can update multiple 'links'es in a single query
    Given transaction closes
    Given connection open schema transaction for database: typedb
    Given typeql schema query
      """
      define
        person plays friendship:youngest-friend, plays friendship:oldest-friend;
        friendship relates youngest-friend, relates oldest-friend;
      """
    Given transaction commits

    Given connection open write transaction for database: typedb
    Given typeql write query
      """
      insert
        $f0 isa friendship, has ref 0,
          links (oldest-friend: $p0, youngest-friend: $p1, friend: $p0);
        $f1 isa friendship, has ref 1,
          links (oldest-friend: $p1, youngest-friend: $p0, friend: $p2);
        $p0 isa person, has ref 0;
        $p1 isa person, has ref 1;
        $p2 isa person, has ref 2;
      """
#    Given get answers of typeql read query
#      """
#      match $f isa friendship, links ($r: $p);
#      """
#    Given uniquely identify answer concepts
#      | f         | p         | r                                |
#      | key:ref:0 | key:ref:0 | label:friendship:friend          |
#      | key:ref:0 | key:ref:0 | label:friendship:oldest-friend   |
#      | key:ref:0 | key:ref:1 | label:friendship:youngest-friend |
#      | key:ref:1 | key:ref:2 | label:friendship:friend          |
#      | key:ref:1 | key:ref:1 | label:friendship:oldest-friend   |
#      | key:ref:1 | key:ref:0 | label:friendship:youngest-friend |

    When get answers of typeql write query
      """
      match
        $p isa person, has ref 1;
        $f isa friendship, links (youngest-friend: $p);
      """
    Then uniquely identify answer concepts
      | f         | p         |
      | key:ref:0 | key:ref:1 |
#    When get answers of typeql read query
#      """
#      match $f isa friendship, links ($r: $p);
#      """
#    Then uniquely identify answer concepts
#      | f         | p         | r                                |
#      | key:ref:0 | key:ref:1 | label:friendship:friend          |
#      | key:ref:0 | key:ref:1 | label:friendship:oldest-friend   |
#      | key:ref:0 | key:ref:1 | label:friendship:youngest-friend |
#      | key:ref:1 | key:ref:2 | label:friendship:friend          |
#      | key:ref:1 | key:ref:1 | label:friendship:oldest-friend   |
#      | key:ref:1 | key:ref:0 | label:friendship:youngest-friend |
#
#    When get answers of typeql write query
#      """
#      insert
#        $p isa person, has ref 3;
#      match
#        $f isa friendship, links (youngest-friend: $p-youngest);
#      update
#        $f links (youngest-friend: $p, oldest-friend: $p-youngest);
#      """
#    Then uniquely identify answer concepts
#      | f         | p         | p-youngest |
#      | key:ref:0 | key:ref:3 | key:ref:1  |
#      | key:ref:1 | key:ref:3 | key:ref:0  |
#    When get answers of typeql read query
#      """
#      match $f isa friendship, links ($r: $p);
#      """
#    Then uniquely identify answer concepts
#      | f         | p         | r                                |
#      | key:ref:0 | key:ref:1 | label:friendship:friend          |
#      | key:ref:0 | key:ref:1 | label:friendship:oldest-friend   |
#      | key:ref:0 | key:ref:3 | label:friendship:youngest-friend |
#      | key:ref:1 | key:ref:2 | label:friendship:friend          |
#      | key:ref:1 | key:ref:0 | label:friendship:oldest-friend   |
#      | key:ref:1 | key:ref:3 | label:friendship:youngest-friend |
#
#    When typeql write query
#      """
#      match
#        $p1 isa person, has ref 1;
#        $p3 isa person, has ref 3;
#        $f isa friendship, links (youngest-friend: $p3);
#      update
#        $f links (youngest-friend: $p1), links (friend: $p3);
#      """
#    When get answers of typeql read query
#      """
#      match $f isa friendship, links ($r: $p);
#      """
#    Then uniquely identify answer concepts
#      | f         | p         | r                                |
#      | key:ref:0 | key:ref:3 | label:friendship:friend          |
#      | key:ref:0 | key:ref:1 | label:friendship:oldest-friend   |
#      | key:ref:0 | key:ref:1 | label:friendship:youngest-friend |
#      | key:ref:1 | key:ref:3 | label:friendship:friend          |
#      | key:ref:1 | key:ref:0 | label:friendship:oldest-friend   |
#      | key:ref:1 | key:ref:1 | label:friendship:youngest-friend |
#
#    When typeql write query
#      """
#      match
#        $p0 isa person, has ref 0;
#        $p2 isa person, has ref 2;
#        $f isa friendship, links (oldest-friend: $_);
#      update
#        $f links (youngest-friend: $p0), links (friend: $p2);
#      """
#    When get answers of typeql read query
#      """
#      match $f isa friendship, links ($r: $p);
#      """
#    Then uniquely identify answer concepts
#      | f         | p         | r                                |
#      | key:ref:0 | key:ref:2 | label:friendship:friend          |
#      | key:ref:0 | key:ref:1 | label:friendship:oldest-friend   |
#      | key:ref:0 | key:ref:0 | label:friendship:youngest-friend |
#      | key:ref:1 | key:ref:2 | label:friendship:friend          |
#      | key:ref:1 | key:ref:0 | label:friendship:oldest-friend   |
#      | key:ref:1 | key:ref:0 | label:friendship:youngest-friend |
