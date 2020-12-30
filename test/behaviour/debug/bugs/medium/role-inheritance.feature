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
        plays employment:employee,
        plays part-time-employment:part-time-employee,
        owns ref @key;
      company sub entity,
        plays employment:employer,
        plays part-time-employment:employer,
        owns ref @key;
      employment sub relation,
        relates employee,
        relates employer,
        owns ref @key;
      part-time-employment sub employment,
        relates part-time-employee as employee;
        # inherits `employer`
      ref sub attribute, value long;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write


  Scenario: relation subtypes can be matched using inherited and overridden roles
    Given graql insert
      """
      insert
      $x isa person, has ref 0;
      $y isa company, has ref 1;
      $r (part-time-employee: $x, employer: $y) isa part-time-employment, has ref 2;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    # Direct role works:
    Then get answers of graql query
      """
      match $r (part-time-employee: $x) isa part-time-employment;
      """
    Then answer size is: 1
    # Inherited and Override role works:
    Then get answers of graql query
      """
      match $r (employee: $x) isa part-time-employment;
      """
    # Inherited but not overriden does not work:
    Then get answers of graql query
      """
      match $r (employer: $x) isa part-time-employment;
      """
    Then answer size is: 1
