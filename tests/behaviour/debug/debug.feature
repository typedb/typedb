# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

#noinspection CucumberUndefinedStep
Feature: TypeQL Query with Expressions

  Background: Open connection, create driver, create database
    Given typedb starts
    Given connection is open: false
    Given connection opens with default authentication
    Given connection is open: true
    Given connection create database: typedb3
    Given connection has database: typedb3


  Scenario Outline: Driver processes attributes of type <value-type> correctly
    Given connection open schema transaction for database: typedb3
    Given typeql schema query
      """
      define entity person, owns typed; attribute typed, value <value-type>;
      """
    Given transaction commits
    Given connection open write transaction for database: typedb3
    Given typeql write query
      """
      insert $p isa person, has typed <value>;
      """
    When get answers of typeql read query
      """
      match $_ isa person, has $a;
      """
    Then answer type is: concept rows
    Then answer query type is: read
    Then answer size is: 1

    Then answer get row(0) get variable(a) is type: false
    Then answer get row(0) get variable(a) is instance: true
    Then answer get row(0) get variable(a) is value: false
    Then answer get row(0) get variable(a) is entity type: false
    Then answer get row(0) get variable(a) is relation type: false
    Then answer get row(0) get variable(a) is attribute type: false
    Then answer get row(0) get variable(a) is role type: false
    Then answer get row(0) get variable(a) is entity: false
    Then answer get row(0) get variable(a) is relation: false
    Then answer get row(0) get variable(a) is attribute: true

    Then answer get row(0) get variable(a) as attribute
    Then answer get row(0) get variable(a) get label: typed
    Then answer get row(0) get instance(a) get label: typed
    Then answer get row(0) get instance(a) get type get label: typed
    Then answer get row(0) get attribute(a) get label: typed
    Then answer get row(0) get attribute(a) get type get label: typed
    Then answer get row(0) get attribute(a) get type is attribute: false
    Then answer get row(0) get attribute(a) get type is entity type: false
    Then answer get row(0) get attribute(a) get type is relation type: false
    Then answer get row(0) get attribute(a) get type is attribute type: true
    Then answer get row(0) get attribute(a) get type is role type: false

    Then answer get row(0) get attribute(a) get type get value type: <value-type>
    Then answer get row(0) get attribute(a) get <value-type>
    Then answer get row(0) get attribute(a) is boolean: <is-boolean>
    Then answer get row(0) get attribute(a) is integer: <is-integer>
    Then answer get row(0) get attribute(a) is double: <is-double>
    Then answer get row(0) get attribute(a) is decimal: <is-decimal>
    Then answer get row(0) get attribute(a) is string: <is-string>
    Then answer get row(0) get attribute(a) is date: <is-date>
    Then answer get row(0) get attribute(a) is datetime: <is-datetime>
    Then answer get row(0) get attribute(a) is datetime-tz: <is-datetime-tz>
    Then answer get row(0) get attribute(a) is duration: <is-duration>
    Then answer get row(0) get attribute(a) is struct: false
    Then answer get row(0) get attribute(a) try get value is: <value>
    Then answer get row(0) get attribute(a) try get <value-type> is: <value>
    Then answer get row(0) get attribute(a) try get value is not: <not-value>
    Then answer get row(0) get attribute(a) try get <value-type> is not: <not-value>
    Then answer get row(0) get attribute(a) get value is: <value>
    Then answer get row(0) get attribute(a) get <value-type> is: <value>
    Then answer get row(0) get attribute(a) get value is not: <not-value>
    Then answer get row(0) get attribute(a) get <value-type> is not: <not-value>
    Examples:
      | value-type  | value                                        | not-value                            | is-boolean | is-integer | is-double | is-decimal | is-string | is-date | is-datetime | is-datetime-tz | is-duration |
      | decimal     | 1234567890.0001234567890dec                  | 1234567890.001234567890dec           | false      | false      | false     | true       | false     | false   | false       | false          | false       |
