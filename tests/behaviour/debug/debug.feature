# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

#noinspection CucumberUndefinedStep
Feature: Debugging
  Background: Open connection and create a simple extensible schema
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true
    Given connection reset database: typedb
    Given connection open schema transaction for database: typedb

  # Test @values with trivially castable types via pure TypeQL
  Scenario: Double attribute with @values containing integers via TypeQL - schema accepted
    Given typeql schema query
      """
      define
      entity person owns score;
      attribute score value double @values(10, 20, 30);
      """
    Given transaction commits
    Given connection open write transaction for database: typedb
    # 10 (integer) should be valid - castable to double and in @values
    Given typeql write query
      """
      insert $p isa person, has score 10;
      """
    # 15 should fail (not in @values)
    Then typeql write query; fails
      """
      insert $p isa person, has score 15;
      """

  # Test @range with trivially castable types via pure TypeQL
  Scenario: Double attribute with @range containing integers via TypeQL - schema accepted
    Given typeql schema query
      """
      define
      entity player owns rating;
      attribute rating value double @range(0..100);
      """
    Given transaction commits
    Given connection open write transaction for database: typedb
    # 50 (integer) should be valid - within range
    Given typeql write query
      """
      insert $p isa player, has rating 50;
      """
    # 150 should fail (outside range)
    Then typeql write query; fails
      """
      insert $p isa player, has rating 150;
      """

  # Test @values with non-compatible types via TypeQL: should fail gracefully
  Scenario: Setting string @values on integer attribute via TypeQL should fail gracefully
    Then typeql schema query; fails
      """
      define
      attribute age value integer @values("young", "old");
      """

  # Test @values with non-compatible types via TypeQL: should fail gracefully
  Scenario: Setting integer @values on string attribute via TypeQL should fail gracefully
    Then typeql schema query; fails
      """
      define
      attribute name value string @values(1, 2, 3);
      """
