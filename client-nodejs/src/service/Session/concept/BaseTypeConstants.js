/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const META_TYPE = "META_TYPE";
const ATTRIBUTE_TYPE = "ATTRIBUTE_TYPE";
const RELATIONSHIP_TYPE = "RELATIONSHIP_TYPE";
const ENTITY_TYPE = "ENTITY_TYPE";
const ENTITY = "ENTITY";
const ATTRIBUTE = "ATTRIBUTE";
const RELATIONSHIP = "RELATIONSHIP";
const ROLE = "ROLE";
const RULE = "RULE";

const SCHEMA_CONCEPTS = new Set([RULE, ROLE, ATTRIBUTE_TYPE, RELATIONSHIP_TYPE, ENTITY_TYPE]);
const TYPES = new Set([ATTRIBUTE_TYPE, RELATIONSHIP_TYPE, ENTITY_TYPE]);
const THINGS = new Set([ATTRIBUTE, RELATIONSHIP, ATTRIBUTE, ENTITY]);

module.exports = {
    baseType: {
        META_TYPE,
        ATTRIBUTE,
        ATTRIBUTE_TYPE,
        ROLE,
        RULE,
        RELATIONSHIP,
        RELATIONSHIP_TYPE,
        ENTITY,
        ENTITY_TYPE
    },
    set: {
        SCHEMA_CONCEPTS, TYPES, THINGS
    }
};
