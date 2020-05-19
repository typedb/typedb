#
# Copyright (C) 2020 Grakn Labs
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
    Given connection delete all keyspaces
    Given connection open sessions for keyspaces:
      | test_define |
    Given transaction is initialised
    Given the integrity is validated
    Given graql define
      """
      define
      person sub entity, plays employee, has name, key email;
      employment sub relation, relates employee;
      name sub attribute, value string;
      email sub attribute, value string;
      """
    Given the integrity is validated


  Scenario: define entity subtype inherits 'has' from supertypes
    Given graql define
      """
      define child sub person;
      """
    Given the integrity is validated

    When get answers of graql query
      """
      match $x has name; get;
      """
    Then concept identifiers are
      |     | check | value  |
      | PER | label | person |
      | CHD | label | child  |
    Then uniquely identify answer concepts
      | x   |
      | PER |
      | CHD |


  Scenario: define entity inherits 'key' from supertypes
    Given graql define
      """
      define child sub person;
      """
    Given the integrity is validated

    When get answers of graql query
      """
      match $x key email; get;
      """

    Then concept identifiers are
      |     | check | value  |
      | PER | label | person |
      | CHD | label | child  |
    Then uniquely identify answer concepts
      | x   |
      | PER |
      | CHD |



  Scenario: define additional 'plays' is visible from all children
    Given graql define
      """
      define employment sub relation, relates employer;
      """
    Given the integrity is validated

    Given graql define
      """
      define
      child sub person;
      person sub entity, plays employer;
      """
    Given the integrity is validated

    When get answers of graql query
      """
      match $x type child, plays $r; get;
      """
    Then concept identifiers are
      |             | check | value            |
      | EMPLOYEE    | label | employee         |
      | EMPLOYER    | label | employer         |
      | CHILD       | label | child            |
    Then uniquely identify answer concepts
      | x     | r           |
      | CHILD | EMPLOYEE    |
      | CHILD | EMPLOYER    |
      | CHILD | NAME_OWNER  |
      | CHILD | EMAIL_OWNER |

