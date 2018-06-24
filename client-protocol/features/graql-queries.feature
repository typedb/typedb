Feature: Graql Queries
  As a Grakn Developer, I should be able to interact with a Grakn knowledge base using Graql queries

    Background: A knowledge base containing types and instances
        Given a knowledge base
        And schema `person sub entity, has name; name sub attribute, datatype string;`
        And data `$alice isa person, has name "Alice";`

    Scenario: Valid Define Query
        When the user issues `define $x label dog sub entity;`
        Then the type "dog" is in the knowledge base
        And return a response with new concepts

    Scenario: Redundant Define Query
        When the user issues `define $x label person sub entity;`
        Then return a response with existing concepts

    Scenario: Valid Insert Query
        When the user issues `insert $bob isa person, has name "Bob";`
        Then the instance with name "Bob" is in the knowledge base
        And return a response with new concepts

    Scenario: Invalid Insert Query
        When the user issues `insert $dunstan isa dog, has name "Dunstan";`
        Then return an error

    Scenario: Get Query With Empty Response
        When the user issues `match $x isa person, has name "Precy"; get;`
        Then the response has no results

    Scenario: Get Query With Non-Empty Response
        When the user issues `match $x isa person, has name "Alice"; get;`
        Then the response has 1 result

    Scenario: Aggregate Ask Query With False Response
        When the user issues `match $x has name "Precy"; aggregate ask;`
        Then the response is `False`

    Scenario: Aggregate Ask Query With True Response
        When the user issues `match $x has name "Alice"; aggregate ask;`
        Then the response is `True`

    Scenario: Aggregate Query
        When the user issues `match $x isa person; aggregate count;`
        Then the response is `1`

    Scenario: Compute Query
        When the user issues `compute count in person;`
        Then the response is `1`

    Scenario: Successful Undefine Query
        Given schema `dog sub entity;`
        When the user issues `undefine dog sub entity;`
        Then the response is empty

    Scenario: Unsuccessful Undefine Query
        When the user issues `undefine person sub entity;`
        Then return an error

    Scenario: Delete Query for non Existent Concept
        When the user issues `match $x has name "Precy"; delete $x;`
        Then the response is empty

    Scenario: Inference on by default
        Given schema `weird-rule sub rule when { $person has name "Alice"; } then { $person has name "A"; };`
        When the user issues `match $x has name "A"; get;`
        Then the response has 1 result

    Scenario: Inference can be disabled
        Given schema `weird-rule sub rule when { $person has name "Alice"; } then { $person has name "A"; };`
        And inference is disabled
        When the user issues `match $x has name "A"; get;`
        Then the response has no results
