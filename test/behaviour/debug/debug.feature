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


  Background: Set up database
    Given reasoning schema
      """
      define

      person sub entity,
          plays team:leader,
          plays team:member,
          owns string-attribute,
          owns unrelated-attribute,
          owns age,
          owns is-old;

      tortoise sub entity,
          owns age,
          owns is-old;

      soft-drink sub entity,
          owns retailer;

      team sub relation,
          relates leader,
          relates member,
          owns string-attribute;

      string-attribute sub attribute, value string;
      retailer sub attribute, value string;
      age sub attribute, value long;
      is-old sub attribute, value boolean;
      unrelated-attribute sub attribute, value string;
      """


  Scenario: Querying for anonymous attributes with predicates finds the correct answers
    Given reasoning schema
      """
      define
      rule people-have-a-specific-age: when {
        $x isa person;
      } then {
        $x has age 10;
      };
      """
    Given reasoning data
      """
      insert
      $geY isa person;
      """
    Given verifier is initialised
    Given reasoning query
      """
      match $x has age > 20;
      """
    Then verify answer size is: 0
    Then verify answers are sound
    Then verify answers are complete
    Given reasoning query
      """
      match $x has age > 5;
      """
    Then verify answer size is: 1
    Then verify answers are sound
    Then verify answers are complete
