/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.hal;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Type;
import ai.grakn.util.Schema;
import com.theoryinpractise.halbuilder.api.Representation;

/**
 * Utils class used by HALBuilders
 *
 * @author Marco Scoppetta
 */
public class HALUtils {

    final static String EXPLORE_CONCEPT_LINK = "explore";

    // - Edges names

    final static String ISA_EDGE = "isa";
    final static String SUB_EDGE = "sub";
    final static String OUTBOUND_EDGE = "OUT";
    final static String INBOUND_EDGE = "IN";
    final static String HAS_ROLE_EDGE = "has-role";
    final static String HAS_RESOURCE_EDGE = "has-resource";
    final static String PLAYS_ROLE_EDGE = "plays-role";

    // - State properties

    final static String ID_PROPERTY = "_id";
    final static String TYPE_PROPERTY = "_type";
    final static String BASETYPE_PROPERTY = "_baseType";
    final static String DIRECTION_PROPERTY = "_direction";
    final static String VALUE_PROPERTY = "_value";
    final static String NAME_PROPERTY = "_name";


    static Schema.BaseType getBaseType(Instance instance) {
        if (instance.isEntity()) {
            return Schema.BaseType.ENTITY;
        } else if (instance.isRelation()) {
            return Schema.BaseType.RELATION;
        } else if (instance.isResource()) {
            return Schema.BaseType.RESOURCE;
        } else if (instance.isRule()) {
            return Schema.BaseType.RULE;
        } else {
            throw new RuntimeException("Unrecognized base type of " + instance);
        }
    }

    static Schema.BaseType getBaseType(Type type) {
        if (type.isEntityType()) {
            return Schema.BaseType.ENTITY_TYPE;
        } else if (type.isRelationType()) {
            return Schema.BaseType.RELATION_TYPE;
        } else if (type.isResourceType()) {
            return Schema.BaseType.RESOURCE_TYPE;
        } else if (type.isRuleType()) {
            return Schema.BaseType.RULE_TYPE;
        } else if (type.isRoleType()) {
            return Schema.BaseType.ROLE_TYPE;
        } else if (type.getName().equals(Schema.MetaSchema.CONCEPT.getName())) {
            return Schema.BaseType.TYPE;
        } else {
            throw new RuntimeException("Unrecognized base type of " + type);
        }
    }

    static void generateConceptState(Representation resource, Concept concept){

        resource.withProperty(ID_PROPERTY, concept.getId().getValue());

        if (concept.isInstance()) {
            Instance instance = concept.asInstance();
            resource.withProperty(TYPE_PROPERTY, instance.type().getName().getValue())
                    .withProperty(BASETYPE_PROPERTY, getBaseType(instance).name());
        } else {
            resource.withProperty(BASETYPE_PROPERTY, getBaseType(concept.asType()).name());
        }

        if (concept.isResource()) {
            resource.withProperty(VALUE_PROPERTY, concept.asResource().getValue());
        }
        if(concept.isType()){
            resource.withProperty(NAME_PROPERTY, concept.asType().getName().getValue());
        }
    }
}
