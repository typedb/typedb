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
      """
    Given for each session, transaction commits
    # each scenario specialises the schema further
    Given for each session, open transactions of type: write


  Scenario: a rule can infer a relation based on ownership of any instance of a specific attribute type
    Given for each session, graql define
      """
      define
      year sub attribute, value long, plays employment:favourite-year;
      employment relates favourite-year;
      rule kronenbourg-employs-anyone-with-a-name: when {
        $x isa company, has name "Kronenbourg";
        $p isa person, has name $n;
        $y 1664 isa year;
      } then {
        (employee: $p, employer: $x, favourite-year: $y) isa employment;
      };
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
      $x isa company, has name "Kronenbourg";
      $p isa person, has name "Ronald";
      $p2 isa person, has name "Prasanth";
      $y 1664 isa year;
      """
    Given for each session, transaction commits
    Given for each session, open transactions of type: write
    Then materialised database is completed
    Given for each session, transaction commits
    Given for each session, open transactions of type: read
    Then for graql query
      """
      match
        $x 1664 isa year;
        ($x, employee: $p, employer: $y) isa employment;
      """
    Then all answers are correct in reasoned database
    Then answer size in reasoned database is: 2
    Then materialised and reasoned databases are the same size

