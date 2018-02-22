/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.engine.rpc;

import ai.grakn.concept.Concept;
import ai.grakn.graql.GraqlConverter;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.util.CommonUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Converts to GRPC result format. This is a special format for {@link Answer}s, but JSON strings for everything else.
 *
 * @author Felix Chapman
 */
class GrpcConverter implements GraqlConverter<Object, QueryResult> {

    private GrpcConverter() {

    }

    public static GrpcConverter get() {
        return new GrpcConverter();
    }

    @Override
    public QueryResult complete(Object builder) {
        if (builder instanceof GraknOuterClass.Answer) {
            return QueryResult.newBuilder().setAnswer((GraknOuterClass.Answer) builder).build();
        } else {
            // If not an answer, convert to JSON
            return QueryResult.newBuilder().setOtherResult(Printers.json().convert(builder)).build();
        }
    }

    @Override
    public Concept build(Concept concept) {
        return concept;
    }

    @Override
    public Boolean build(boolean bool) {
        return bool;
    }

    @Override
    public Optional<?> build(Optional<?> optional) {
        return optional;
    }

    @Override
    public Collection<?> build(Collection<?> collection) {
        return collection;
    }

    @Override
    public Map<?, ?> build(Map<?, ?> map) {
        return map;
    }

    @Override
    public GraknOuterClass.Answer build(Answer answer) {
        GraknOuterClass.Answer.Builder answerRps = GraknOuterClass.Answer.newBuilder();
        answer.forEach((var, concept) -> {
            GraknOuterClass.Concept conceptRps = makeConcept(concept);
            answerRps.putAnswer(var.getValue(), conceptRps);
        });
        return answerRps.build();
    }

    @Override
    public Object buildDefault(Object object) {
        return object;
    }

    private GraknOuterClass.Concept makeConcept(Concept concept) {
        return GraknOuterClass.Concept.newBuilder()
                .setId(GraknOuterClass.ConceptId.newBuilder().setValue(concept.getId().getValue()).build())
                .setBaseType(getBaseType(concept))
                .build();
    }

    private GraknOuterClass.BaseType getBaseType(Concept concept) {
        if (concept.isEntityType()) {
            return GraknOuterClass.BaseType.EntityType;
        } else if (concept.isRelationshipType()) {
            return GraknOuterClass.BaseType.RelationshipType;
        } else if (concept.isAttributeType()) {
            return GraknOuterClass.BaseType.AttributeType;
        } else if (concept.isEntity()) {
            return GraknOuterClass.BaseType.Entity;
        } else if (concept.isRelationship()) {
            return GraknOuterClass.BaseType.Relationship;
        } else if (concept.isAttribute()) {
            return GraknOuterClass.BaseType.Attribute;
        } else if (concept.isRole()) {
            return GraknOuterClass.BaseType.Role;
        } else if (concept.isRule()) {
            return GraknOuterClass.BaseType.Rule;
        } else if (concept.isType()) {
            return GraknOuterClass.BaseType.MetaType;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }
    }
}
