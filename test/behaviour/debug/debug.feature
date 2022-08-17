#
# Copyright (C) 2022 Vaticle
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
    Given connection create database: typedb
    Given connection open schema session for database: typedb
    Given session opens transaction of type: write

    Given typeql define
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

    Given session opens transaction of type: write



  Scenario: two attribute types can own each other in a cycle
    Given typeql define
      """
      define
      nickname sub attribute, value string, owns surname, owns middlename;
      surname sub attribute, value string, owns nickname;
      middlename sub attribute, value string, owns firstname;
      firstname sub attribute, value string, owns surname;
      """
    Then get answers of typeql match
      """
      match $a sub attribute, owns $b; $b sub attribute, owns $a;
      """
    Then uniquely identify answer concepts
      | a              | b              |
      | label:nickname | label:surname  |
      | label:surname  | label:nickname |
    Then get answers of typeql match
      """
      match $a owns $b; $b owns $a;
      """
    Then uniquely identify answer concepts
      | a              | b              |
      | label:nickname | label:surname  |
      | label:surname  | label:nickname |