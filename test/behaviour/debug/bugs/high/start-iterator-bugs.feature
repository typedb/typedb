#
# Copyright (C) 2020 Grakn Labs
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#noinspection CucumberUndefinedStep
Feature: Graql Match Query

  Background: Open connection and create a simple extensible schema
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: grakn
    Given connection open schema session for database: grakn
    Given session opens transaction of type: write
    Given the integrity is validated
    Given graql define
      """
      define
      person sub entity,
        plays friendship:friend,
        plays employment:employee,
        owns name,
        owns age,
        owns ref @key;
      company sub entity,
        plays employment:employer,
        owns name,
        owns ref @key;
      friendship sub relation,
        relates friend,
        owns ref @key;
      employment sub relation,
        relates employee,
        relates employer,
        owns ref @key;
      name sub attribute, value string;
      age sub attribute, value long;
      ref sub attribute, value long;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: write


  ##################
  # SCHEMA QUERIES #
  ##################

  # TODO fails because it does not find Roles when using $x as starting point.
  # TODO Discuss starting at all Types including vs excluding Role Types. This may have implications for `match $x sub thing`
  Scenario: 'relates' can be used to retrieve all the roles of a relation type
    When get answers of graql query
      """
      match employment relates $x;
      """
    And concept identifiers are
      |     | check | value               |
      | EME | label | employment:employee |
      | EMR | label | employment:employer |
    Then uniquely identify answer concepts
      | x   |
      | EME |
      | EMR |

    # TODO this is buggy  if the QP starts from $x, as `relation:role` is not included as a starting point
  Scenario: 'plays' can be used to match roles that a particular type can play
    When get answers of graql query
      """
      match person plays $x;
      """
    And concept identifiers are
      |     | check | value               |
      | FRI | label | friendship:friend   |
      | EMP | label | employment:employee |
    Then uniquely identify answer concepts
      | x   |
      | FRI |
      | EMP |