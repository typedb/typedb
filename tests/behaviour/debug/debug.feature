# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space
  Background: Open connection and create a simple extensible schema
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true
    Given connection reset database: typedb

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.



  Scenario: Test unary minus sign
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
    Given connection open write transaction for database: typedb
    Given typeql write query
      """
      insert
      $x isa age 16;
      """
    Given transaction commits

    Given connection open read transaction for database: typedb
    When get answers of typeql read query
      """
      match
        let $const = -10;
      """
    Then uniquely identify answer concepts
      | const            |
      |value:integer:-10 |

    When get answers of typeql read query
      """
      match
        $x isa age;
        let $const = -10;
        let $plus-negative = $x + -10;
        let $minus-negative = $x - -10;
      """
    Then uniquely identify answer concepts
      | x           | const             | plus-negative   | minus-negative   |
      | attr:age:16 | value:integer:-10 | value:integer:6 | value:integer:26 |

