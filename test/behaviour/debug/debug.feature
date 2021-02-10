#
# Copyright (C) 2021 Grakn Labs
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

Feature: Debugging Space

  Background: Open connection and create a simple extensible schema
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: grakn
    Given connection open schema session for database: grakn
    Given session opens transaction of type: write

    Given graql define
      """
      define
      person sub entity, plays employment:employee, owns name, owns email @key;
      employment sub relation, relates employee, relates employer;
      name sub attribute, value string;
      email sub attribute, value string, regex ".+@\w+\..+";
      abstract-type sub entity, abstract;
      """
    Given transaction commits

    Given session opens transaction of type: write
  Scenario: undefining abstract on a type that is already non-abstract does nothing
    When graql undefine
      """
      undefine person abstract;
      """
    Then transaction commits

    When session opens transaction of type: read
    When get answers of graql match
      """
      match
        $x type person;
        not { $x abstract; };
      """
    Then uniquely identify answer concepts
      | x            |
      | label:person |
