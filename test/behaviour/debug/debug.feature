Feature: Debugging Space

  Background:
    Given connection has been opened
    Given connection delete all keyspaces
    Given connection does not have any keyspace

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  Scenario: one keyspace, one session, re-committing transaction throws
    When connection create keyspace:
      | grakn   |
    Given connection open session for keyspace:
      | grakn   |
    When for each session, open transaction of type:
      | write   |
    Then for each session, transaction commits successfully: true
    Then for each session, transaction commits successfully: false