Feature: Relation Inference Resolution

  Background: Set up databases for resolution testing
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: typedb
    Given connection open schema session for database: typedb
    Given session opens transaction of type: write
    Given typeql define
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
    Given transaction commits
    # each scenario specialises the schema further
    Given open transactions of type: write

  #######################
  # BASIC FUNCTIONALITY #
  #######################

  Scenario: a relation can be inferred on all concepts of a given type
    Given typeql define
      """
      define
      dog sub entity;
      rule people-are-employed: when {
        $p isa person;
      } then {
        (employee: $p) isa employment;
      };
      """
    Given transaction commits
    Given connection close all sessions
    Given connection open data session for database: typedb
    Given session opens transactions of type: write
    Given typeql insert
      """
      insert
      $x isa person;
      $y isa dog;
      $z isa person;
      """
    Given transaction commits
    Given session opens transaction of type: write
    Then materialise all possible inferences
    Given session opens transaction of type: read
    Then for typeql query
      """
      match
        $x isa person;
        ($x) isa employment;
      """
    Then all answers are correct in reasoned database
    Then answer size in reasoned database is: 2
    Then transaction closes
    Given session opens transaction of type: read
