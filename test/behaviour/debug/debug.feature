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
      gene-id sub attribute, value string;

      gene sub entity,
        owns gene-id @key,
        plays gene-disease-association:gene,
        plays gene-protein-encoding:gene;

      disease sub entity,
        plays gene-disease-association:disease;

      protein sub entity,
        plays gene-protein-encoding:protein,
        plays protein-protein-interaction:protein;

      gene-protein-encoding sub relation,
        relates gene,
        relates protein;

      protein-protein-interaction sub relation,
        relates protein;

      gene-disease-association sub relation,
        relates gene,
        relates disease;
      """
    Given transaction commits
    Given the integrity is validated
    Given session opens transaction of type: write


  Scenario: new entity types can be defined
    Given connection close all sessions
    Given connection open data session for database: grakn
    Given session opens transaction of type: write
    When graql insert
      """
      insert
      $g1 isa gene, has gene-id '1789'; $p1 isa protein;
      $g2 isa gene, has gene-id '1776'; $d1 isa disease; $p1 isa protein; $p2 isa protein;
      $gpe (gene: $g1, protein:$p1) isa gene-protein-encoding;
      $ppi (protein: $p1, protein: $p2) isa protein-protein-interaction;
      $gpe2 (gene: $g2, protein: $p2) isa gene-protein-encoding;
      $gndis (gene: $g2, disease: $d1) isa gene-disease-association;
      """
    Then transaction commits
    Then the integrity is validated
    When session opens transaction of type: read
    When get answers of graql match
      """
      match
      $g1 isa gene, has gene-id '1789'; $p1 isa protein;
      $gpe ($g1, $p1) isa gene-protein-encoding;
      $ppi ($p1, $p2) isa protein-protein-interaction;
      $gpe2 ($g2, $p2) isa gene-protein-encoding;
      $gndis ($g2, $d1) isa gene-disease-association;
      """
    Then the integrity is validated


