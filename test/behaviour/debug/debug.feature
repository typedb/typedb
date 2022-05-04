#
# Copyright (C) 2021 Vaticle
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
      person sub entity,
        plays friendship:friend,
        owns name @key;
      friendship sub relation,
        relates friend,
        owns ref @key;
      name sub attribute, value string;
      ref sub attribute, value long;
      """
    Given transaction commits

    Given connection close all sessions
    Given connection open data session for database: typedb
    Given session opens transaction of type: write


  Scenario: repeated role players can be deleted from a relation
    Given get answers of typeql insert
      """
      insert
      $x isa person, has name "Alex";
      $y isa person, has name "Bob";
      $r (friend: $x, friend: $x, friend: $y) isa friendship, has ref 0;
      """
    Then uniquely identify answer concepts
      | x             | y            | r         |
      | key:name:Alex | key:name:Bob | key:ref:0 |
    Given transaction commits
    When session opens transaction of type: write
    When get answers of typeql match
      """
      match
        $r (friend: $x, friend: $x) isa friendship;
      """
    Then answer size is: 1
    Then transaction commits

    When session opens transaction of type: read
    When get answers of typeql match
      """
      match $r (friend: $x) isa friendship;
      """
    Then uniquely identify answer concepts
      | r         | x            |
      | key:ref:0 | key:name:Bob |