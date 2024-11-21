# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space
  Background: Debug
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true

  Scenario: Debugging
    Given connection open write transaction for database: tpcc
    When get answers of typeql read query
      """
      match
        $w isa WAREHOUSE, has W_ID 5;
        $d isa DISTRICT, has D_ID 59;
        $order links (district: $d), isa ORDER, has O_ID 150;
        $item has I_ID 351;
      """
    Then answer size is: 1
  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.