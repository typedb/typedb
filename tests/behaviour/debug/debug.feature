# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  Background: Open connection and create a simple extensible schema
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true
    Given connection reset database: typedb
    Given connection open schema transaction for database: typedb

    Given typeql schema query
      """
      define
      entity person;
      """
    Given transaction commits

    Given connection open write transaction for database: typedb

  Scenario: Min and Max with integers, decimals, and doubles
    When get answers of typeql read query
      """
      match
        let $a = max(2, -3);
        let $b = min(2, -3);
        let $c = max(10.2dec, 13.5dec);
        let $d = min(10.2dec, 13.5dec);
        let $e = max(10.2, 13.5);
        let $f = min(10.2, 13.5);
      select
        $a, $b, $c, $d, $e, $f;
      """
    Then uniquely identify answer concepts
      | a                | b                 | c                  | d                  | e                | f                |
      | value:integer:2  | value:integer:-3  | value:decimal:13.5dec | value:decimal:10.2dec | value:double:13.5 | value:double:10.2 |
    Then typeql read query; fails with a message containing: "Built-in expression function 'min' expects '2' arguments but received '0' arguments."
      """
      match let $x = min();
      """
    Then typeql read query; fails with a message containing: "Built-in expression function 'min' expects '2' arguments but received '1' arguments."
      """
      match let $x = min(10);
      """
    Then typeql read query; fails with a message containing: "Built-in expression function 'min' expects '2' arguments but received '3' arguments."
      """
      match let $x = min(10, 12, 14);
      """
    Then typeql read query; fails with a message containing: "Built-in expression function 'max' expects '2' arguments but received '0' arguments."
      """
      match let $x = max();
      """
    Then typeql read query; fails with a message containing: "Built-in expression function 'max' expects '2' arguments but received '1' arguments."
      """
      match let $x = max(10);
      """
    Then typeql read query; fails with a message containing: "Built-in expression function 'max' expects '2' arguments but received '3' arguments."
      """
      match let $x = max(10, 12, 14);
      """