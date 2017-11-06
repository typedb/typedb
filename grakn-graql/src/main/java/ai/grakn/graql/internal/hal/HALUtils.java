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

import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.theoryinpractise.halbuilder.api.Representation;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Utils class used by HALBuilders
 *
 * @author Marco Scoppetta
 */
public class HALUtils {

    final static String EXPLORE_CONCEPT_LINK = "explore";

    // - Edges names

    final static String ISA_EDGE = Schema.EdgeLabel.ISA.getLabel();
    final static String SUB_EDGE = Schema.EdgeLabel.SUB.getLabel();
    final static String OUTBOUND_EDGE = "OUT";
    final static String INBOUND_EDGE = "IN";
    final static String RELATES_EDGE = Schema.EdgeLabel.RELATES.getLabel();
    final static String HAS_EDGE = "has";
    final static String PLAYS_EDGE = Schema.EdgeLabel.PLAYS.getLabel();
    final static String ASSERTION_URL = "?keyspace=%s&query=%s&limitEmbedded=%s";


    // - State properties

    public final static String ID_PROPERTY = "_id";
    public final static String TYPE_PROPERTY = "_type";
    public final static String BASETYPE_PROPERTY = "_baseType";
    public final static String DIRECTION_PROPERTY = "_direction";
    public final static String VALUE_PROPERTY = "_value";
    public final static String DATATYPE_PROPERTY = "_dataType";
    public final static String NAME_PROPERTY = "_name";

    public final static String INFERRED_RELATIONSHIP = "INFERRED_RELATIONSHIP";
    public final static String GENERATED_RELATIONSHIP = "generated-relationship";
    public final static String IMPLICIT_PROPERTY = "_implicit";


    static Schema.BaseType getBaseType(Thing thing) {
        if (thing.isEntity()) {
            return Schema.BaseType.ENTITY;
        } else if (thing.isRelationship()) {
            return Schema.BaseType.RELATIONSHIP;
        } else if (thing.isAttribute()) {
            return Schema.BaseType.ATTRIBUTE;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised base type of " + thing);
        }
    }

    static Schema.BaseType getBaseType(SchemaConcept schemaConcept) {
        if (schemaConcept.isEntityType()) {
            return Schema.BaseType.ENTITY_TYPE;
        } else if (schemaConcept.isRelationshipType()) {
            return Schema.BaseType.RELATIONSHIP_TYPE;
        } else if (schemaConcept.isAttributeType()) {
            return Schema.BaseType.ATTRIBUTE_TYPE;
        } else if (schemaConcept.isRule()) {
            return Schema.BaseType.RULE;
        } else if (schemaConcept.isRole()) {
            return Schema.BaseType.ROLE;
        } else if (schemaConcept.getLabel().equals(Schema.MetaSchema.THING.getLabel())) {
            return Schema.BaseType.TYPE;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised base type of " + schemaConcept);
        }
    }

    static void generateConceptState(Representation resource, Concept concept, boolean inferred) {

        resource.withProperty(ID_PROPERTY, concept.getId().getValue());

        if (concept.isThing()) {
            Thing thing = concept.asThing();
            resource.withProperty(TYPE_PROPERTY, thing.type().getLabel().getValue());
            String baseType = (inferred) ? INFERRED_RELATIONSHIP : getBaseType(thing).name();
            resource.withProperty(BASETYPE_PROPERTY, baseType);
        }

        if (concept.isAttribute()) {
            resource.withProperty(VALUE_PROPERTY, concept.asAttribute().getValue());
            resource.withProperty(DATATYPE_PROPERTY, concept.asAttribute().dataType().getName());
        }

        if (concept.isSchemaConcept()) {
            resource.withProperty(BASETYPE_PROPERTY, getBaseType(concept.asSchemaConcept()).name());
            resource.withProperty(NAME_PROPERTY, concept.asSchemaConcept().getLabel().getValue());
            resource.withProperty(IMPLICIT_PROPERTY, ((SchemaConcept) concept).isImplicit());
            if (concept.isAttributeType()) {
                String dataType = Optional.ofNullable(concept.asAttributeType().getDataType()).map(x -> x.getName()).orElse("");
                resource.withProperty(DATATYPE_PROPERTY, dataType);
            }
        }
    }

    static String computeHrefInferred(Concept currentConcept, Keyspace keyspace, int limit) {

        VarPattern relationPattern = Graql.var();
        Set<Pattern> idPatterns = new HashSet<>();

        for (Map.Entry<Role, Set<Thing>> entry : currentConcept.asRelationship().allRolePlayers().entrySet()) {
            for (Thing var : entry.getValue()) {
                Var rolePlayer = Graql.var();
                relationPattern = relationPattern.rel(entry.getKey().getLabel().getValue(), rolePlayer);
                idPatterns.add(rolePlayer.asUserDefined().id(var.getId()));
            }
        }
        relationPattern = relationPattern.isa(currentConcept.asRelationship().type().getLabel().getValue());

        Pattern pattern = relationPattern;
        for (Pattern idPattern : idPatterns) {
            pattern = pattern.and(idPattern);
        }

        String withoutURL = String.format(ASSERTION_URL, keyspace, Graql.match(pattern).get().toString(), limit);
        String URL = REST.WebPath.Dashboard.EXPLAIN;

        return URL + withoutURL;
    }

}
