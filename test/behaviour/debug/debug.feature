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
  Scenario: subtypes trigger rules based on their parents; parent types don't trigger rules based on their children
    Given reasoning schema
      """
      define

      person sub entity,
          owns name,
          plays performance:writer,
          plays performance:performer,
          plays film-production:writer,
          plays film-production:actor;

      child sub person;

      performance sub relation,
          relates writer,
          relates performer;

      film-production sub relation,
          relates writer,
          relates actor;

      name sub attribute, value string;

      rule performance-to-film-production: when {
          $x isa child;
          $y isa person;
          (performer:$x, writer:$y) isa performance;
      } then {
          (actor:$x, writer:$y) isa film-production;
      };
      """
    Given reasoning data
      """
      insert
      $x isa child, has name "a";
      $y isa person, has name "b";
      $z isa person, has name "a";
      $w isa person, has name "b2";
      $v isa child, has name "a";

      (performer:$x, writer:$z) isa performance;  # child - person   -> satisfies rule
      (performer:$y, writer:$z) isa performance;  # person - person  -> doesn't satisfy rule
      (performer:$x, writer:$v) isa performance;  # child - child    -> satisfies rule
      (performer:$y, writer:$v) isa performance;  # person - child   -> doesn't satisfy rule
      """
    Given verifier is initialised
    Given reasoning query
      """
      match
        $x isa person;
        $y isa person;
        (actor: $x, writer: $y) isa film-production;
      """
    # Answers are (actor:$x, writer:$z) and (actor:$x, writer:$v)
    Then verify answer size is: 2
    Then verify answers are sound
    Then verify answers are complete
    Given reasoning query
      """
      match
        $x isa person;
        $y isa person;
        (actor: $x, writer: $y) isa film-production;
        $y has name 'a';
      """
    Then verify answer size is: 2
    Then verify answers are sound
    Then verify answers are complete
    Given reasoning query
      """
      match
        $x isa person;
        $y isa child;
        (actor: $x, writer: $y) isa film-production;
      """
    # Answer is (actor:$x, writer:$v) ONLY
    Then verify answer size is: 1
    Then verify answers are sound
    Then verify answers are complete
    Given reasoning query
      """
      match
        $x isa person;
        $y isa child;
        (actor: $x, writer: $y) isa film-production;
        $y has name 'a';
      """
    Then verify answer size is: 1
    Then verify answers are sound
    Then verify answers are complete
    Given reasoning query
      """
      match
        $x isa child;
        $y isa person;
        (actor: $x, writer: $y) isa film-production;
      """
    # Answers are (actor:$x, writer:$z) and (actor:$x, writer:$v)
    Then verify answer size is: 2
    Then verify answers are sound
    Then verify answers are complete
    Given reasoning query
      """
      match
        $x isa child;
        $y isa person;
        (actor: $x, writer: $y) isa film-production;
        $y has name 'a';
      """
    Then verify answer size is: 2
    Then verify answers are sound
    Then verify answers are complete

