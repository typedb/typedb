# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  Background:
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true
    Given connection has 0 databases
    Given connection create database: typedb
    Given connection open schema transaction for database: typedb




  Scenario: Plays data is revalidated when new cardinality constraints appear
    # Setup

    When create attribute type: ref
    When attribute(ref) set value type: string
    When create relation type: rel0
    When relation(rel0) create role: role00
    When relation(rel0) get role(role00) set annotation: @abstract
    When relation(rel0) get role(role00) set annotation: @card(0..)
    When relation(rel0) create role: role0
    When relation(rel0) get role(role0) set annotation: @abstract
    When relation(rel0) get role(role0) set annotation: @card(0..)
    When create relation type: rel1
    When relation(rel1) set supertype: rel0
    When relation(rel1) create role: role1
    When relation(rel1) get role(role1) set specialise: role0
    When relation(rel1) get role(role1) set annotation: @card(0..)
    When create relation type: rel2
    When relation(rel2) set supertype: rel1
    When relation(rel2) set owns: ref
    When relation(rel2) get owns(ref) set annotation: @key
    When relation(rel2) create role: anchor
    When create entity type: anchor
    When entity(anchor) set plays: rel2:anchor
    When create entity type: ent0
    When entity(ent0) set owns: ref
    When entity(ent0) get owns(ref) set annotation: @key
    When create entity type: ent1
    When create entity type: ent2
    When entity(ent1) set supertype: ent0
    When entity(ent2) set supertype: ent1
    When entity(ent0) set plays: rel0:role0
    When entity(ent1) set plays: rel1:role1
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(0..1)
    When transaction commits

    When connection open write transaction for database: typedb
    When $anchor = entity(anchor) create new instance
    When $rel1 = relation(rel2) create new instance with key(ref): rel1
    When relation $rel1 add player for role(anchor): $anchor
    When $rel2 = relation(rel2) create new instance with key(ref): rel2
    When relation $rel2 add player for role(anchor): $anchor
    When $rel3 = relation(rel2) create new instance with key(ref): rel3
    When relation $rel3 add player for role(anchor): $anchor
    When transaction commits

    # Direct cardinality changes validation

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) create new instance with key(ref): ent2
    When transaction commits

    When connection open schema transaction for database: typedb
    When entity(ent1) get plays(rel1:role1) set annotation: @card(1..)
    Then transaction commits; fails with a message containing: "card(1..)"

    When connection open schema transaction for database: typedb
    When entity(ent1) get plays(rel1:role1) set annotation: @card(1..1)
    Then transaction commits; fails with a message containing: "card(1..1)"

    When connection open schema transaction for database: typedb
    When entity(ent1) get plays(rel1:role1) set annotation: @card(0..1)
    Then transaction commits

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(1..)
    Then transaction commits; fails with a message containing: "card(1..)"

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(1..1)
    Then transaction commits; fails with a message containing: "card(1..1)"

    When connection open schema transaction for database: typedb
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(1..1)
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel1 = relation(rel2) get instance with key(ref): rel1
    When $rel2 = relation(rel2) get instance with key(ref): rel2
    When relation $rel1 add player for role(role1): $ent2
    When relation $rel2 add player for role(role1): $ent2
    Then transaction commits; fails with a message containing: "@card(0..1)"

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel1 = relation(rel2) get instance with key(ref): rel1
    When relation $rel1 add player for role(role1): $ent2
    When transaction commits

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(1..)
    When entity(ent0) get plays(rel0:role0) set annotation: @card(1..1)
    When entity(ent1) get plays(rel1:role1) set annotation: @card(1..1)
    When entity(ent1) get plays(rel1:role1) set annotation: @card(1..)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(1..1)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..)
    When entity(ent1) get plays(rel1:role1) unset annotation: @card
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(1..1)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..1)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(1..)
    Then transaction commits

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(2..3)
    Then transaction commits; fails with a message containing: "card(2..3)"

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(1..3)
    When entity(ent1) get plays(rel1:role1) set annotation: @card(1..2)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..2)
    Then transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel2 = relation(rel2) get instance with key(ref): rel2
    When relation $rel2 add player for role(role1): $ent2
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel3 = relation(rel2) get instance with key(ref): rel3
    When relation $rel3 add player for role(role1): $ent2
    Then transaction commits; fails with a message containing: "@card(1..2)"

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(1..2)
    When entity(ent1) get plays(rel1:role1) set annotation: @card(1..3)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..3)
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel3 = relation(rel2) get instance with key(ref): rel3
    When relation $rel3 add player for role(role1): $ent2
    Then transaction commits; fails with a message containing: "@card(1..2)"

    # Default cardinality effect validation (no validation as it creates @card(0..)!

    When connection open schema transaction for database: typedb
    Then entity(ent1) get plays(rel1:role1) unset annotation: @card
    Then transaction commits

    When connection open schema transaction for database: typedb
    Then entity(ent1) get plays(rel1:role1) set annotation: @card(1..3)
    Then transaction commits

    # Set sibling capability effect validation

    When connection open schema transaction for database: typedb
    When relation(rel1) create role: role2
    When relation(rel1) get role(role2) set specialise: role0
    When relation(rel1) get role(role2) set annotation: @card(0..)
    When entity(ent2) set plays: rel1:role2
    When entity(ent2) get plays(rel1:role2) set annotation: @card(1..)
    Then transaction commits; fails with a message containing: "@card(1..)"

    When connection open schema transaction for database: typedb
    When relation(rel1) get role(role2) set specialise: role0
    When entity(ent0) get plays(rel0:role0) set annotation: @card(0..2)
    Then transaction commits

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(0..1)
    Then transaction commits; fails with a message containing: "@card(0..1)"

    When connection open schema transaction for database: typedb
    When entity(ent2) get plays(rel1:role2) set annotation: @card(1..1)
    Then transaction commits; fails with a message containing: "@card(1..1)"

    When connection open schema transaction for database: typedb
    When entity(ent2) get plays(rel1:role2) set annotation: @card(0..1)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(0..2)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(0..2)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(0..2)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(0..1)
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel1 = relation(rel2) get instance with key(ref): rel1
    When relation $rel1 add player for role(role2): $ent2
    Then transaction commits; fails with a message containing: "@card(0..1)"

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(2..3)
    When entity(ent0) get plays(rel0:role0) set annotation: @card(1..3)
    When entity(ent0) get plays(rel0:role0) set annotation: @card(0..3)
    When entity(ent1) get plays(rel1:role1) set annotation: @card(2..2)
    When entity(ent1) get plays(rel1:role1) set annotation: @card(1..2)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(0..3)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(0..3)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(0..3)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(0..1)
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel1 = relation(rel2) get instance with key(ref): rel1
    When relation $rel1 add player for role(role2): $ent2
    Then transaction commits

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(2..3)
    When entity(ent0) get plays(rel0:role0) set annotation: @card(1..3)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(0..1)
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel2 = relation(rel2) get instance with key(ref): rel2
    When relation $rel2 add player for role(role2): $ent2
    Then transaction commits; fails with a message containing: "@card(0..1)"

    When connection open schema transaction for database: typedb
    When entity(ent2) get plays(rel1:role2) set annotation: @card(1..10)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..10)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(1..10)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(1..3)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(1..10)
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel2 = relation(rel2) get instance with key(ref): rel2
    When relation $rel2 add player for role(role2): $ent2
    Then transaction commits; fails with a message containing: "@card(1..3)"

    When connection open schema transaction for database: typedb
    When entity(ent0) get plays(rel0:role0) set annotation: @card(2..4)
    When entity(ent0) get plays(rel0:role0) set annotation: @card(1..4)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(1..4)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..10)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..4)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(1..10)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(1..4)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(1..10)
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel2 = relation(rel2) get instance with key(ref): rel2
    When relation $rel2 add player for role(role2): $ent2
    Then transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel2 = relation(rel2) get instance with key(ref): rel2
    When relation $rel2 remove player for role(role1): $ent2
    When $rel1 = relation(rel2) get instance with key(ref): rel1
    When relation $rel1 remove player for role(role2): $ent2
    Then transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel1 = relation(rel2) get instance with key(ref): rel1
    When relation $rel1 remove player for role(role1): $ent2
    Then transaction commits; fails with a message containing: "@card(XXX)"

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel2 = relation(rel2) get instance with key(ref): rel2
    When relation $rel2 remove player for role(role2): $ent2
    Then transaction commits; fails with a message containing: "@card(YYUY)"

    When connection open schema transaction for database: typedb
    When entity(ent2) get plays(rel1:role2) set annotation: @card(0..)
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(1..4)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..4)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(1..4)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(0..1)
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel2 = relation(rel2) get instance with key(ref): rel2
    When relation $rel2 remove player for role(role2): $ent2
    Then transaction commits

    When connection open schema transaction for database: typedb
    When entity(ent1) get plays(rel1:role1) unset annotation: @card
    Then entity(ent2) get constraints for played role(rel0:role0) contain: @card(1..4)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel0:role0) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(1..4)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role1) do not contain: @card(0..1)
    Then entity(ent2) get constraints for played role(rel1:role1) contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(1..4)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(1..2)
    Then entity(ent2) get constraints for played role(rel1:role2) contain: @card(0..)
    Then entity(ent2) get constraints for played role(rel1:role2) do not contain: @card(0..1)
    When transaction commits

    When connection open write transaction for database: typedb
    When $ent2 = entity(ent2) get instance with key(ref): ent2
    When $rel1 = relation(rel2) get instance with key(ref): rel1
    When relation $rel1 remove player for role(role1): $ent2
    Then transaction commits; fails with a message containing: "@card(vvv)"