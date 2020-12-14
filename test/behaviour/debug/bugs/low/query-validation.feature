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

#noinspection CucumberUndefinedStep
Feature: Graql Match Query

  Background: Open connection and create a simple extensible schema
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: grakn
    Given connection open schema session for database: grakn
    Given session opens transaction of type: write
    Given the integrity is validated
    Given graql define
      """
      define
      person sub entity,
        plays friendship:friend,
        plays employment:employee,
        owns name,
        owns age,
        owns ref @key;
      company sub entity,
        plays employment:employer,
        owns name,
        owns ref @key;
      friendship sub relation,
        relates friend,
        owns ref @key;
      employment sub relation,
        relates employee,
        relates employer,
        owns ref @key;
      name sub attribute, value string;
      age sub attribute, value long;
      ref sub attribute, value long;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: write


  # TODO should throw exceptions when labels don't exist -- unsatisfiable check/exception?
  Scenario: when matching by a type whose label doesn't exist, an error is thrown
    Then graql match; throws exception
      """
      match $x isa ganesh;
      """
    Then the integrity is validated


  # TODO should throw exceptions when labels don't exist -- unsatisfiable check/exception?
  Scenario: when matching by a relation type whose label doesn't exist, an error is thrown
    Then graql match; throws exception
      """
      match ($x, $y) isa $type; $type type jakas-relacja;
      """
    Then the integrity is validated


  # TODO should throw exceptions when labels don't exist -- unsatisfiable check/exception?
  Scenario: when matching a non-existent type label to a variable from a generic 'isa' query, an error is thrown
    Then graql match; throws exception
      """
      match $x isa $type; $type type polok;
      """
    Then the integrity is validated


   # TODO should this throw in the new semantics? Should be "unsatisfiable"
  Scenario: an error is thrown when matching an entity type as if it were a role type
    Then graql match; throws exception
      """
      match (person: $x) isa relation;
      """
    Then the integrity is validated


   # TODO should this throw in the new semantics? Should be "unsatisfiable"
  Scenario: an error is thrown when matching an entity as if it were a relation
    Then graql match; throws exception
      """
      match ($x) isa person;
      """
    Then the integrity is validated


  # TODO should this throw if using a label that doesn't exist
  Scenario: an error is thrown when matching a non-existent type label as if it were a relation type
    Then graql match; throws exception
      """
      match ($x) isa bottle-of-rum;
      """
    Then the integrity is validated


  #  TODO should this throw if using a label that doesn't exist
  Scenario: when matching a role type that doesn't exist, an error is thrown
    Then graql match; throws exception
      """
      match (rolein-rolein-rolein: $rolein) isa relation;
      """
    Then the integrity is validated


   # TODO this should either throw or be unsatisfiable
  Scenario: an error is thrown when matching by attribute ownership, when the owned thing is actually an entity
    Then graql match; throws exception
      """
      match $x has person "Luke";
      """
    Then the integrity is validated


  # TODO this should either throw or be unsatisfiable
  Scenario: when matching by an attribute ownership, if the owner can't actually own it, an empty result is returned
    When get answers of graql query
      """
      match $x isa company, has age $n;
      """
    Then answer size is: 0


  # TODO this should either throw or be unsatisfiable
  Scenario: an error is thrown when matching by attribute ownership, when the owned type label doesn't exist
    Then graql match; throws exception
      """
      match $x has bananananananana "rama";
      """
    Then the integrity is validated