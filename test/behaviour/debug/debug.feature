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

#noinspection CucumberUndefinedStep
Feature: Recursion Resolution

  In some cases, the inferences made by a rule are used to trigger further inferences by the same rule.
  This test feature verifies that so-called recursive inference works as intended.

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
    Given for each session, open transactions of type: write

  # TODO: re-enable all steps when materialisation is possible (may be an infinite graph?) (#75)
  Scenario: when relations' and attributes' inferences are mutually recursive, the inferred concepts can be retrieved
    Given for each session, graql define
      """
      define

      word sub entity,
          plays inheritance:subtype,
          plays inheritance:supertype,
          plays pair:prep,
          plays pair:pobj,
          owns name;

      f sub word;
      o sub word;

      inheritance sub relation,
          relates supertype,
          relates subtype;

      pair sub relation,
          relates prep,
          relates pobj,
          owns typ,
          owns name;

      name sub attribute, value string;
      typ sub attribute, value string;

      rule inference-all-pairs: when {
          $x isa word;
          $y isa word;
          $x has name != 'f';
          $y has name != 'o';
      } then {
          (prep: $x, pobj: $y) isa pair;
      };

      rule inference-pairs-ff: when {
          $f isa f;
          (subtype: $prep, supertype: $f) isa inheritance;
          (subtype: $pobj, supertype: $f) isa inheritance;
          $p (prep: $prep, pobj: $pobj) isa pair;
      } then {
          $p has name 'ff';
      };

      rule inference-pairs-fo: when {
          $f isa f;
          $o isa o;
          (subtype: $prep, supertype: $f) isa inheritance;
          (subtype: $pobj, supertype: $o) isa inheritance;
          $p (prep: $prep, pobj: $pobj) isa pair;
      } then {
          $p has name 'fo';
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

      $f isa f, has name "f";
      $o isa o, has name "o";

      $aa isa word, has name "aa";
      $bb isa word, has name "bb";
      $cc isa word, has name "cc";

      (supertype: $o, subtype: $aa) isa inheritance;
      (supertype: $o, subtype: $bb) isa inheritance;
      (supertype: $o, subtype: $cc) isa inheritance;

      $pp isa word, has name "pp";
      $qq isa word, has name "qq";
      # $rr isa word, has name "rr";
      # $rr2 isa word, has name "rr";

      (supertype: $f, subtype: $pp) isa inheritance;
      (supertype: $f, subtype: $qq) isa inheritance;
      # (supertype: $f, subtype: $rr) isa inheritance;
      # (supertype: $f, subtype: $rr2) isa inheritance;
      """
    Given for each session, transaction commits
    Given for each session, open transactions of type: write
    Then materialised database is completed
    Given for each session, transaction commits
    Given for each session, open transactions with reasoning of type: read
    Then for graql query
      """
      match $p isa pair, has name 'ff';
      """
    Then all answers are correct in reasoned database
    Then answer size in reasoned database is: 4
    Then for each session, transaction closes
    Given for each session, open transactions with reasoning of type: read
    Then for graql query
      """
      match $p isa pair;
      """
    Then all answers are correct in reasoned database
    Then answer size in reasoned database is: 36
    Then materialised and reasoned databases are the same size
