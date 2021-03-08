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
    Given connection open data session for database: grakn
    Given session opens transaction of type: write

  Scenario: deleting everything in a complex pattern
    Given connection close all sessions
    Given connection open schema session for database: grakn
    Given session opens transaction of type: write
    Given graql define
      """
      define
      lastname sub attribute, value string;
      person sub entity, owns lastname;
      """
    Given transaction commits

    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    Given graql insert
      """
      insert
      $x isa person,
        has lastname "Smith",
        has name "Alex";
      $y isa person,
        has lastname "Smith",
        has name "John";
      $r (friend: $x, friend: $y) isa friendship, has ref 1;
      $r1 (friend: $x, friend: $y) isa friendship, has ref 2;
      $reflexive (friend: $x, friend: $x) isa friendship, has ref 3;
      """
    Given transaction commits

    When session opens transaction of type: write
    When graql delete
      """
      match
        $x isa person, has name "Alex", has lastname $n;
        $y isa person, has name "John", has lastname $n;
        $refl (friend: $x, friend: $x) isa friendship, has ref $r1; $r1 3;
        $f1 (friend: $x, friend: $y) isa friendship, has ref $r2; $r2 1;
      delete
        $x isa person, has $n;
        $y isa person, has $n;
        $refl (friend: $x, friend: $x) isa friendship, has $r1;
        $f1 (friend: $x, friend: $y) isa friendship, has $r2;
      """
    Then transaction commits
