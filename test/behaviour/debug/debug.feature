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

      animal sub entity;
      mammal sub animal;
      reptile sub animal;
      tortoise sub reptile;

      person sub mammal,
          owns name,
          owns email,
          plays marriage:spouse;

      man sub person,
          plays marriage:husband;

      woman sub person,
          plays marriage:wife;

      dog sub mammal,
          owns name,
          owns label;

      name sub attribute, value string;

      email sub attribute, value string;

      marriage sub relation,
          relates husband,
          relates wife,
          relates spouse;

      shape sub entity,
          owns perimeter,
          owns area,
          abstract;

      triangle sub shape,
          owns label;

      right-angled-triangle sub triangle,
          owns hypotenuse-length;

      square sub shape;

      perimeter sub attribute, value double;

      area sub attribute, value double;

      hypotenuse-length sub attribute, value double;

      label sub attribute, value string;
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
      match $p owns perimeter;
      """
    Then uniquely identify answer concepts
      | p         |
      | label:triangle       |
      | label:right-angled-triangle       |
      | label:square |
      | label:shape |