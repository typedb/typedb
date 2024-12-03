# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  Background: Open connection and create a simple extensible schema
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true

  Scenario: Debugging
    Given connection open read transaction for database: tpcc
    When get answers of typeql read query
      """
match
$d isa DISTRICT, has D_ID 41;
$o links (customer: $c, district: $d), isa ORDER, has O_ID $o_id, has O_NEW_ORDER true;
$c isa CUSTOMER, has C_ID $c_id;
select $o_id, $c_id;
      """
    Then answer size is: 5