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
  # TODO: re-enable all steps once schema queries are resolvable (#75)
  Scenario: all roleplayers and their types can be retrieved from a relation
    Given for each session, graql define
      """
      define

      military-person sub person;
      colonel sub military-person;

      rule armed-forces-employ-the-military: when {
        $x isa company, has name "Armed Forces";
        $y isa military-person;
      } then {
        (employee: $y, employer: $x) isa employment;
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
      $x isa company, has name "Armed Forces";
      $y isa colonel;
      $z isa colonel;
      """
    Then materialised database is completed
    Given for each session, transaction commits
    Given for each session, open transactions with reasoning of type: read
    Then for graql query
      """
      match
        ($x, $y) isa employment;
        $x isa $type;
      """
    Then all answers are correct in reasoned database
    # (2 colonels * 5 supertypes of colonel * 1 company)
    # + (1 company * 3 supertypes of company * 2 colonels)
    Then answer size in reasoned database is: 16
    Then materialised and reasoned databases are the same size



