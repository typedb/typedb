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

  Background: Set up databases for resolution testing
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: reasoned
    Given connection create database: materialised
    Given connection open schema sessions for databases:
      | reasoned     |
      | materialised |
    Given for each session, open transactions of type: write
    Given for each session, graql define
      """
      define

      person sub entity,
        owns name,
        owns age,
        plays friendship:friend,
        plays employment:employee;

      company sub entity,
        owns name,
        plays employment:employer;

      place sub entity,
        owns name,
        plays location-hierarchy:subordinate,
        plays location-hierarchy:superior;

      friendship sub relation,
        relates friend;

      employment sub relation,
        relates employee,
        relates employer;

      location-hierarchy sub relation,
        relates subordinate,
        relates superior;

      name sub attribute, value string;
      age sub attribute, value long;

            area sub place;
      city sub place;
      country sub place;
      continent sub place;

      rule location-hierarchy-transitivity: when {
          (superior: $a, subordinate: $b) isa location-hierarchy;
          (superior: $b, subordinate: $c) isa location-hierarchy;
      } then {
          (superior: $a, subordinate: $c) isa location-hierarchy;
      };
      """
    Given for each session, transaction commits
    # each scenario specialises the schema further
    Given for each session, open transactions of type: write

  #####################
  # NEGATION IN MATCH #
  #####################

  # Negation is currently handled by Reasoner, even inside a match clause.


  Scenario: negation can filter out an unwanted entity type from part of a chain of matched relations
    Given for each session, graql define
      """
      define
      dog sub entity, plays friendship:friend;
      """
    Given for each session, transaction commits
    Given connection close all sessions
    Given connection open data sessions for databases:
      | reasoned     |
      | materialised |
    Given for each session, open transactions of type: write
    Given for each session, graql insert
      """
      insert
      $c isa person;
      $d isa person;
      $z isa dog;

      (friend: $c, friend: $d) isa friendship;
      (friend: $d, friend: $z) isa friendship;
      """
    Given for each session, transaction commits
    Given for each session, open transactions with reasoning of type: read
    Then for graql query
      """
      match
        (friend: $c, friend: $d) isa friendship;
        not {$c isa dog;};
      """
    # Eliminates (cdzd, zdzd)
    Then answer size in reasoned database is: 2
    Then for each session, transaction closes
    Given for each session, open transactions with reasoning of type: read
    Then answer set is equivalent for graql query
      """
      match
        (friend: $c, friend: $d) isa friendship;
        $c isa person;
      """


#  Scenario: negation can filter out an unwanted connection between two concepts from a chain of matched relations
#    Given for each session, graql define
#      """
#      define
#      dog sub entity, owns name, plays friendship:friend;
#      """
#    Given for each session, transaction commits
#    Given connection close all sessions
#    Given connection open data sessions for databases:
#      | reasoned     |
#      | materialised |
#    Given for each session, open transactions of type: write
#    Given for each session, graql insert
#      """
#      insert
#      $a isa person, has name "a";
#      $b isa person, has name "b";
#      $c isa person, has name "c";
#      $d isa person, has name "d";
#      $z isa dog, has name "z";
#
#      (friend: $a, friend: $b) isa friendship;
#      (friend: $b, friend: $c) isa friendship;
#      (friend: $c, friend: $d) isa friendship;
#      (friend: $d, friend: $z) isa friendship;
#      """
#    Given for each session, transaction commits
#    Given for each session, open transactions with reasoning of type: read
#    Then for graql query
#    """
#      match
#        (friend: $a, friend: $b) isa friendship;
#        (friend: $b, friend: $c) isa friendship;
#      """
#    # aba, abc
#    # bab, bcb, bcd
#    # cba, cbc, cdc, cdz
#    # dcb, dcd, dzd
#    # zdc, zdz
#    Given answer size in reasoned database is: 14
#    Then for each session, transaction closes
#    Given for each session, open transactions with reasoning of type: read
#    Then for graql query
#      """
#      match
#        (friend: $a, friend: $b) isa friendship;
#        not {(friend: $b, friend: $z) isa friendship;};
#        (friend: $b, friend: $c) isa friendship;
#        $z isa dog;
#      """
#    # (d,z) is a friendship so we eliminate results where $b is 'd': these are (cdc, cdz, zdc, zdz)
#    Then answer size in reasoned database is: 10
#    Then for each session, transaction closes
#    Given for each session, open transactions with reasoning of type: read
#    Then answer set is equivalent for graql query
#      """
#      match
#        (friend: $a, friend: $b) isa friendship;
#        (friend: $b, friend: $c) isa friendship;
#        $z isa dog;
#        not {$b has name "d";};
#      """
#
#
