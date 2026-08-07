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
    Given connection open schema transaction for database: typedb
    Given typeql schema query
    """
    define
    fun four_or_five() -> { integer }:
    match { let $x = 4; } or { let $x = 5; };
    return { $x };

    """
#    Given typeql schema query
#      """
#      define
#      fun names_helper($len: integer) -> { string }:
#        match
#          {
#            $len > 1;
#            let $a_ in names_helper($len - 1);
#            let $a = "a" + $a_;
#          } or {
#            let $a = "a";
#          };
#        return { $a };
#      fun names() -> { string }:
#        match let $a in names_helper(20);
#        return { $a };
#      """
    Given transaction commits
