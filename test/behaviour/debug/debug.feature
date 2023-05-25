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
    Given typedb starts
    Given connection opens without authentication
    Given reasoning schema
      """
      define

      person sub entity,
        owns unrelated-attribute,
        owns sub-string-attribute,
        owns name,
        owns age,
        owns is-old;

      tortoise sub entity,
        owns age,
        owns is-old;

      soft-drink sub entity,
        owns name,
        owns retailer,
        owns price;

      string-attribute sub attribute, value string, abstract;
      sub-string-attribute sub string-attribute;
      retailer sub attribute, value string;
      age sub attribute, value long;
      name sub attribute, value string;
      is-old sub attribute, value boolean;
      price sub attribute, value double;
      unrelated-attribute sub attribute, value string;
      """
    # each scenario specialises the schema further

  Scenario: a negation can filter out variables by equality to another variable with a specified value
    Given reasoning schema
      """
      define
      rule tesco-sells-all-soft-drinks: when {
        $x isa soft-drink;
      } then {
        $x has retailer 'Tesco';
      };

      rule if-ocado-exists-it-sells-all-soft-drinks: when {
        $x isa retailer;
        $x = 'Ocado';
        $y isa soft-drink;
      } then {
        $y has retailer 'Ocado';
      };
      """
    Given reasoning data
      """
      insert
      $x isa soft-drink, has name "Fanta";
      $y isa soft-drink, has name "Tango";
      $r "Ocado" isa retailer;
      """
    Given verifier is initialised
    Given reasoning query
      """
      match
        $x has retailer $r;
        not {
          $r = $unwanted;
          $unwanted = "Ocado";
        };
      """
    # x     | r     |
    # Fanta | Tesco |
    # Tango | Tesco |
    Then verify answer size is: 2
    Then verify answers are sound
    Then verify answers are complete


