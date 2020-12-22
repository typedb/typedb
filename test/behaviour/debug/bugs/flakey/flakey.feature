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

  Scenario: relations between distinct concepts are not retrieved when matching concepts that relate to themselves
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x isa person, has ref 1;
      $y isa person, has ref 2;
      (friend: $x, friend: $y) isa friendship, has ref 0;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match (friend: $x, friend: $x) isa friendship;
      """
    Then answer size is: 0

  Scenario: Relations can be queried with pairings of relation and role types that are not directly related to each other
    Given graql define
      """
      define
      person plays hetero-marriage:husband, plays hetero-marriage:wife;
      marriage sub relation, relates spouse;
      hetero-marriage sub marriage, relates husband as spouse, relates wife as spouse;
      """
    Given transaction commits
    Given the integrity is validated
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $a isa person, has ref 1;
      $b isa person, has ref 2;
      (wife: $a, husband: $b) isa hetero-marriage;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: read
    When get answers of graql query
      """
      match (wife: $x, husband: $y) isa relation;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match (wife: $x, husband: $y) isa marriage;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match (wife: $x, husband: $y) isa hetero-marriage;
      """
    Then answer size is: 1
    When get answers of graql query
      """
      match (spouse: $x, spouse: $y) isa hetero-marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (spouse: $x, spouse: $y) isa marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (spouse: $x, spouse: $y) isa relation;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (role: $x, role: $y) isa hetero-marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (role: $x, role: $y) isa marriage;
      """
    Then answer size is: 2
    When get answers of graql query
      """
      match (role: $x, role: $y) isa relation;
      """
    Then answer size is: 2