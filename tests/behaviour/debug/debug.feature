# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

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
      entity person
        plays friendship:friend,
        plays employment:employee,
        owns name @card(0..),
        owns age @card(0..),
        owns ref @key,
        owns email @unique @card(0..);
      entity company
        plays employment:employer,
        owns name @card(0..),
        owns ref @key;
      relation friendship
        relates friend @card(0..),
        owns ref @key;
      relation employment
        relates employee @card(0..),
        relates employer @card(0..),
        owns ref @key;
      attribute name value string;
      attribute age @independent, value integer;
      attribute ref value integer;
      attribute email value string;
      """
    Given transaction commits

    Given connection open schema transaction for database: typedb


  Scenario: value comparisons can be performed between a 'double' and a 'integer'
    Given typeql schema query
      """
      define
      attribute house-number @independent, value integer;
      attribute length @independent, value double;
      """
    Given transaction commits

    Given connection open write transaction for database: typedb
    Given typeql write query
      """
      insert
      $x isa house-number 1;
      $y isa length 2.0;
      """
    Given transaction commits

    Given connection open read transaction for database: typedb
    When get answers of typeql read query
      """
      match
        $x isa $a;
        $x >= 1;
      """
    Then answer size is: 2
