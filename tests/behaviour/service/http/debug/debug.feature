# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  Background: Open connection, create database
    Given typedb starts
    Given connection is open: false
    Given connection opens with default authentication
    Given connection is open: true
    Given connection has 0 databases
    Given connection create database: typedb
    Given connection has database: typedb

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  Scenario: Transaction read row queries return all concepts correctly
    Given connection open schema transaction for database: typedb
    Given typeql schema query
      """
      define
        entity person, plays parentship:parent, owns name;
        attribute name value string;
        relation parentship, relates parent;
      """
    Given typeql write query
      """
      insert
        $p isa person, has name 'John';
        $pp isa parentship, links ($p);
      """
    Given transaction commits

    When connection open read transaction for database: typedb
    When get answers of typeql read query
      """
      match
        $entity isa $entity-type, has $attribute-type $attribute;
        $relation isa $relation-type, links ($entity);
        $relation-type relates $role-type;
        let $value = $attribute;
      """
    Then answer size is: 1
    Then answer type is: concept rows
    Then answer type is not: concept documents
    Then answer contains document:
    """
    {
        "data": {
          "entity": {
              "kind": "entity",
              "iid": "0x1e00000000000000000000",
              "type": {
                  "kind": "entityType",
                  "label": "person"
              }
          },
          "role-type": {
              "kind": "roleType",
              "label": "parentship:parent"
          },
          "relation": {
              "kind": "relation",
              "iid": "0x1f00000000000000000000",
              "type": {
                  "kind": "relationType",
                  "label": "parentship"
              }
          },
          "relation-type": {
              "kind": "relationType",
              "label": "parentship"
          },
          "attribute-type": {
              "kind": "attributeType",
              "label": "name",
              "valueType": "string"
          },
          "entity-type": {
              "kind": "entityType",
              "label": "person"
          },
          "value": {
              "kind": "value",
              "value": "John",
              "valueType": "string"
          },
          "attribute": {
              "kind": "attribute",
              "value": "John",
              "valueType": "string",
              "type": {
                  "kind": "attributeType",
                  "label": "name",
                  "valueType": "string"
              }
          }
        },
        "provenanceBitArray": [0, 0, 0, 0, 0, 0, 0, 0]
    }
    """