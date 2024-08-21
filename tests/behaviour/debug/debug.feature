# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  Background:
    Given typedb starts
    Given connection opens with default authentication
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: typedb
    Given connection open schema transaction for database: typedb
    # Write schema for the test scenarios
    Given create attribute type: username
    Given attribute(username) set value type: string
    Given create attribute type: email
    Given attribute(email) set value type: string
    Given create entity type: person
    Given entity(person) set owns: username
    Given entity(person) get owns(username) set annotation: @key
    Given entity(person) set owns: email
    Given transaction commits
    Given connection open write transaction for database: typedb

  Scenario: Entity can be created
    When $a = entity(person) create new instance with key(username): alice
    Then entity $a exists
    Then entity $a has type: person
    Then entity(person) get instances contain: $a
    Then transaction commits
    When connection open read transaction for database: typedb
    When $a = entity(person) get instance with key(username): alice
    Then entity(person) get instances contain: $a