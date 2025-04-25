# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

#noinspection CucumberUndefinedStep
Feature: TypeQL Query with Expressions

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
        owns name @key,
        owns age,
        owns height,
        owns weight;
      attribute name @independent, value string;
      attribute age @independent, value integer;
      attribute height @independent, value integer;
      attribute weight @independent, value integer;

      attribute limit-double @independent, value double;
      """
    Given transaction commits


  Scenario: A value variable must have exactly one assignment constraint in the same branch of a pipeline
    Given connection open write transaction for database: typedb
    Given typeql write query
    """
    insert $p isa person, has name "Lisa", has age 10, has height 180;
    """
    Given transaction commits

    Given connection open read transaction for database: typedb
    Then typeql read query; fails with a message containing: "Invalid query containing unbound concept variable v"
    """
      match
        $x isa person, has age $a, has height $h;
        $v == $a;
        $v > $h;
      select
        $x, $v;
      """
    Given transaction closes

    Given connection open read transaction for database: typedb
    Then typeql read query; fails with a message containing: "Variable 'v' cannot be assigned to multiple times in the same branch."
    """
      match
        $x isa person, has age $a, has height $h;
        let $v = $a * 2;
        let $v = $h * 2;
      select
        $x, $v;
      """
    Then typeql read query; fails with a message containing: "The variable 'v' may not be assigned to, as it was already bound in a previous stage"
    """
      match
        $x isa person, has age $a, has height $h;
        let $v = $a * 2;
      match
        let $v = $h * 2;
      select
        $x, $v;
      """
    Then typeql read query; fails with a message containing: "Variable 'v' cannot be assigned to multiple times in the same branch."
    """
      match
        $x isa person, has age $a, has height $h;
        let $v0 = $v; # TODO: Remove. once rebased on Dmitrii's changes
        { let $v = $a * 2; } or { let $v = 0; };
        { let $v = $h * 2; } or { let $v = 1; };
      select
        $x, $v;
      """
    Then typeql read query; fails with a message containing: "The variable 'v' may not be assigned to, as it was already bound in a previous stage"
    """
      match
        $x isa person, has age $a, has height $h;
        let $v0 = $v; # TODO: Remove. once rebased on Dmitrii's changes
        { let $v = $a * 2; } or { let $v = 1; };
      match
        { let $v = $h * 2; } or { let $v = 2; };
      select
        $x, $v;
      """
    Then get answers of typeql read query
    """
      match
        $x isa person, has age $a, has height $h;
        let $v0 = $v; # TODO: Remove. once rebased on Dmitrii's changes
        { let $v = $a * 2; } or { let $v = $h * 2; };
      select
        $v;
      """
    Then uniquely identify answer concepts
      | v                 |
      | value:integer:20  |
      | value:integer:360 |

    Then get answers of typeql read query
    """
      match
        $x isa person, has age $a, has height $h;
        let $v0 = $v; # TODO: Remove. once rebased on Dmitrii's changes
        { let $v = 12; } or {
          let $v1 = $v;
          { let $v = $a * 2; } or { let $v = $h * 2; };
        };
      select
        $v;
      """
    Then uniquely identify answer concepts
      | v                 |
      | value:integer:12  |
      | value:integer:20  |
      | value:integer:360 |


