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
      """
    Given for each session, transaction commits
    # each scenario specialises the schema further
    Given for each session, open transactions of type: write


    # TODO: re-enable all steps when fixed (#75)
  Scenario: when evaluating negation blocks, global subgoals are not updated

  The test highlights a potential issue with eagerly updating global subgoals when branching out to determine whether
  negation conditions are met. When checking negation satisfiability, we are interested in a first answer that can
  prove us wrong - we are not exhaustively exploring all answer options.

  Consequently, if we use the same subgoals as for the main loop, we can end up with a query which answers weren't
  fully consumed but that was marked as visited.

  As a result, if it happens that a negated query has multiple answers and is visited more than a single time
  - because of the admissibility check, answers might be missed.

    Given for each session, graql define
      """
      define

      session sub entity,
          plays reported-fault:parent-session,
          plays unanswered-question:parent-session,
          plays logged-question:parent-session,
          plays diagnosis:parent-session;

      fault sub entity,
          plays reported-fault:relevant-fault,
          plays fault-identification:identified-fault,
          plays diagnosis:diagnosed-fault;

      question sub entity,
          owns response,
          plays fault-identification:identifying-question,
          plays logged-question:question-logged,
          plays unanswered-question:question-not-answered;

      response sub attribute, value string;

      reported-fault sub relation,
          relates relevant-fault,
          relates parent-session;

      logged-question sub relation,
          relates question-logged,
          relates parent-session;

      unanswered-question sub relation,
          relates question-not-answered,
          relates parent-session;

      fault-identification sub relation,
          relates identifying-question,
          relates identified-fault;

      diagnosis sub relation,
          relates diagnosed-fault,
          relates parent-session;

      rule no-response-means-unanswered-question: when {
          $ques isa question;
          (question-logged: $ques, parent-session: $ts) isa logged-question;
          not {
              $ques has response $r;
          };
      } then {
          (question-not-answered: $ques, parent-session: $ts) isa unanswered-question;
      };

      rule determined-fault: when {
          (relevant-fault: $flt, parent-session: $ts) isa reported-fault;
          not {
              (question-not-answered: $ques, parent-session: $ts) isa unanswered-question;
              ($flt, $ques) isa fault-identification;
          };
      } then {
          (diagnosed-fault: $flt, parent-session: $ts) isa diagnosis;
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
      $sesh isa session;
      $q1 isa question;
      $q2 isa question;
      $f1 isa fault;
      $f2 isa fault;
      (relevant-fault: $f1, parent-session: $sesh) isa reported-fault;
      (relevant-fault: $f2, parent-session: $sesh) isa reported-fault;

      (question-logged: $q1, parent-session: $sesh) isa logged-question;
      (question-logged: $q2, parent-session: $sesh) isa logged-question;

      (identified-fault: $f1, identifying-question: $q1) isa fault-identification;
      (identified-fault: $f2, identifying-question: $q2) isa fault-identification;
      """
    Then materialised database is completed
    Given for each session, transaction commits
    Given for each session, open transactions with reasoning of type: read
    Then for graql query
      """
      match (diagnosed-fault: $flt, parent-session: $ts) isa diagnosis;
      """
    Then answer size in reasoned database is: 0
    Then answers are consistent across 5 executions in reasoned database
    Then materialised and reasoned databases are the same size


