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
      entity nothing;
      entity person
        plays friendship:friend,
        plays employment:employee,
        owns name @card(0..),
        owns person-name @card(0..),
        owns age @card(0..1),
        owns karma @card(0..2),
        owns ref @key;
      entity company
        plays employment:employer,
        owns company-name @card(1..1),
        owns description @card(1..1000),
        owns achievement @card(0..1),
        owns company-achievement @card(0..),
        owns ref @key;
      relation friendship
        relates friend @card(0..),
        owns ref @key;
      relation employment
        relates employee,
        relates employer,
        owns ref @key,
        owns start-date @card(0..),
        owns end-date @card(0..);
      attribute name @abstract, value string;
      attribute person-name sub name;
      attribute company-name sub name;
      attribute description, value string;
      attribute age value integer;
      attribute karma value double;
      attribute ref value integer;
      attribute start-date value datetime;
      attribute end-date value datetime;
      attribute achievement @abstract;
      attribute company-achievement sub achievement, value string;
      """
    Given transaction commits

    Given connection open write transaction for database: typedb
    Given typeql write query
      """
      insert
      $n isa nothing;
      $p1 isa person, has person-name "Alice", has person-name "Allie", has age 10, has karma 123.4567891, has ref 0;
      $p2 isa person, has person-name "Bob", has ref 1;
      $c1 isa company, has company-name "TypeDB", has ref 2, has description "Nice and shy guys", has company-achievement "Green BDD tests for fetch";
      $f1 links (friend: $p1, friend: $p2), isa friendship, has ref 3;
      $e1 links (employee: $p1, employer: $c1), isa employment, has ref 4, has start-date 2020-01-01T13:13:13.999, has end-date 2021-01-01;
      """
    Given transaction commits
    Given connection open read transaction for database: typedb


  Scenario: an attribute and a value can be fetched
    When get answers of typeql read query
      """
      match
      $a isa person-name;
      """
    Then answer size is: 3
#    Then answer contains document:
#      """
#      {
#        "person": "Alice"
#      }
#      """
#    Then answer contains document:
#      """
#      {
#        "person": "Allie"
#      }
#      """
#    Then answer contains document:
#      """
#      {
#        "person": "Bob"
#      }
#      """
#
#    When get answers of typeql read query
#      """
#      match
#      $a isa name;
#      let $v = $a;
#      fetch {
#        "value": $v,
#        "attribute": $a
#      };
#      """
#    Then answer size is: 4
#    Then answer contains document:
#      """
#      {
#        "value": "Alice",
#        "attribute": "Alice"
#      }
#      """
#    Then answer contains document:
#      """
#      {
#        "value": "Allie",
#        "attribute": "Allie"
#      }
#      """
#    Then answer contains document:
#      """
#      {
#        "value": "Bob",
#        "attribute": "Bob"
#      }
#      """
#    Then answer contains document:
#      """
#      {
#        "value": "TypeDB",
#        "attribute": "TypeDB"
#      }
#      """
#    Then answer does not contain document:
#      """
#      {
#        "value": "value",
#        "attribute": "value"
#      }
#      """
#    Then answer does not contain document:
#      """
#      {
#        "value": "Allie",
#        "attribute": "Alice"
#      }
#      """
