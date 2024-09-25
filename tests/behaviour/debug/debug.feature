# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  Scenario: Debug
    Given typedb starts
    Given connection opens with default authentication
    Given connection has been opened
    Given connection reset database: typedb
    Given connection open schema transaction for database: typedb
    Given typeql define
      """
    define
      entity person,
        owns age,
        owns ref;
      attribute age value long;
      attribute ref value long;
    """
    Given transaction commits

    Given connection open write transaction for database: typedb
    Given typeql write query
    """
        insert
        $p1 isa person, has age 30, has ref 0;
        $p2 isa person, has age 30, has ref 1;
        $p3 isa person, has age 75, has ref 2;
        """
    Given transaction commits

    Given connection open read transaction for database: typedb
    When get answers of typeql read query
    """
        match $x isa person;
        """
    Then answer size is: 3
    When get answers of typeql read query
    """
        match $x isa person, has age $y;
        """
    Then answer size is: 3

