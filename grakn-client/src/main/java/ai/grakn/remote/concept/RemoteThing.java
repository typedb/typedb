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

package ai.grakn.remote.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.grpc.ConceptProperty;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toSet;

/**
 * @author Felix Chapman
 *
 * @param <Self> The exact type of this class
 * @param <MyType> the type of an instance of this class
 */
abstract class RemoteThing<Self extends Thing, MyType extends Type> extends RemoteConcept<Self> implements Thing {

    @Override
    public final MyType type() {
        return asMyType(getProperty(ConceptProperty.DIRECT_TYPE));
    }

    @Override
    public final Stream<Relationship> relationships(Role... roles) {
        Stream<Concept> concepts;
        if (roles.length != 0) {
            Var roleVar = var("role");
            Set<VarPattern> patterns = Stream.of(roles).map(role -> roleVar.label(role.getLabel())).collect(toSet());
            concepts = query(TARGET.rel(roleVar, ME), or(patterns));
        } else {
            concepts = query(TARGET.rel(ME));
        }
        return concepts.map(Concept::asRelationship);
    }

    @Override
    public final Stream<Role> plays() {
        return query(var().rel(TARGET, ME)).map(Concept::asRole);
    }

    @Override
    public final Self attribute(Attribute attribute) {
        attributeRelationship(attribute);
        return asSelf(this);
    }

    @Override
    public final Relationship attributeRelationship(Attribute attribute) {
        Label label = attribute.type().getLabel();
        Var attributeVar = var("attribute");
        return insert(attributeVar.id(attribute.getId()), ME.has(label, attributeVar, TARGET)).asRelationship();
    }

    @Override
    public final Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        Stream<Label> attributeTypeLabels;
        if (attributeTypes.length > 0) {
            attributeTypeLabels = Stream.of(attributeTypes).map(SchemaConcept::getLabel);
        } else {
            attributeTypeLabels = Stream.of(Schema.MetaSchema.ATTRIBUTE.getLabel());
        }

        Set<VarPattern> patterns = attributeTypeLabels.map(label -> ME.has(label, TARGET)).collect(toImmutableSet());

        return query(or(patterns)).map(Concept::asAttribute);
    }

    @Override
    public final Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        // Cheat by looking up allowed keys
        // TODO: don't cheat
        Set<AttributeType> keys;
        Set<AttributeType> allowedKeys = type().keys().collect(toSet());

        if (attributeTypes.length > 0) {
            keys = Sets.intersection(allowedKeys, ImmutableSet.copyOf(attributeTypes));
        } else {
            keys = allowedKeys;
        }

        return attributes(keys.toArray(new AttributeType[keys.size()]));
    }

    @Override
    public final Self deleteAttribute(Attribute attribute) {
        Label label = attribute.type().getLabel();
        Var attributeVar = var("attribute");
        Match match = tx().graql().match(me(), attributeVar.id(attribute.getId()), ME.has(label, attributeVar, TARGET));
        match.delete(TARGET).execute();
        return asSelf(this);
    }

    @Override
    public final boolean isInferred() {
        return getProperty(ConceptProperty.IS_INFERRED);
    }

    abstract MyType asMyType(Concept concept);
}
