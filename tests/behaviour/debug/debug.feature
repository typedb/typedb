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

  Scenario: Debug test
    When typeql read query
      """
      match 
      let $x = min(10, 12);
      let $y = max(10, 12);
      select $x, $y;
      """