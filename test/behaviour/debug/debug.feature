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
      person sub entity, plays employment:employee, plays income:earner, owns name, owns email @key;
      employment sub relation, relates employee, plays income:source, owns start-date, owns employment-reference-code @key;
      income sub relation, relates earner, relates source;

      name sub attribute, value string;
      email sub attribute, value string;
      start-date sub attribute, value datetime;
      employment-reference-code sub attribute, value string;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: write


  Scenario: new entity types can be defined
    When graql define
      """
      define dog sub entity;
      """
    Then transaction commits
    Then the integrity is validated
    When session opens transaction of type: read
    When get answers of graql query
      """
      match $x type dog;
      """
    Then uniquely identify answer concepts
      | x         |
      | label:dog |