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
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.grpc.ConceptProperty;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static ai.grakn.util.Schema.MetaSchema.THING;

/**
 * @author Felix Chapman
 *
 * @param <Self> The exact type of this class
 * @param <MyType> the type of an instance of this class
 */
abstract class RemoteThing<Self extends Thing, MyType extends Type> extends RemoteConcept implements Thing {

    @Override
    public final MyType type() {
        // TODO: We use a trick here because there's no "direct isa" in Graql and we don't want to use gRPC for this.
        // The direct type of this concept will have the same indirect super-types as the indirect types of this concept
        Var x = var("x");
        GetQuery query = tx().graql().match(var().id(getId()).isa(x)).get();
        Set<MyType> indirectTypes = query.stream()
                .map(answer -> answer.get(x))
                .filter(RemoteThing::notMetaThing)
                .map(this::asMyType)
                .collect(toImmutableSet());
        Predicate<MyType> hasExpectedSups = concept -> concept.sups().collect(toImmutableSet()).equals(indirectTypes);
        Optional<MyType> type = indirectTypes.stream().filter(hasExpectedSups).findAny();
        return type.orElseThrow(() -> CommonUtil.unreachableStatement("Thing has no type"));
    }

    private static boolean notMetaThing(Concept concept) {
        return !concept.isSchemaConcept() || !concept.asSchemaConcept().getLabel().equals(THING.getLabel());
    }

    @Override
    public final Stream<Relationship> relationships(Role... roles) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Role> plays() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Self attribute(Attribute attribute) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Relationship attributeRelationship(Attribute attribute) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        Stream<Label> attributeTypeLabels;
        if (attributeTypes.length > 0) {
            attributeTypeLabels = Stream.of(attributeTypes).map(SchemaConcept::getLabel);
        } else {
            attributeTypeLabels = Stream.of(Schema.MetaSchema.ATTRIBUTE.getLabel());
        }

        Var x = var("x");

        Set<VarPattern> patterns =
                attributeTypeLabels.map(label -> var().id(getId()).has(label, x)).collect(toImmutableSet());

        GetQuery query = tx().graql().match(or(patterns)).get();
        return query.stream().map(answer -> answer.get(x).asAttribute());
    }

    @Override
    public final Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Self deleteAttribute(Attribute attribute) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final boolean isInferred() {
        return getProperty(ConceptProperty.IS_INFERRED);
    }

    abstract MyType asMyType(Concept concept);
}
