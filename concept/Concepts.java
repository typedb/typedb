/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.concept;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.data.Thing;
import grakn.core.concept.data.impl.ThingImpl;
import grakn.core.concept.schema.AttributeType;
import grakn.core.concept.schema.EntityType;
import grakn.core.concept.schema.RelationType;
import grakn.core.concept.schema.Rule;
import grakn.core.concept.schema.ThingType;
import grakn.core.concept.schema.Type;
import grakn.core.concept.schema.impl.AttributeTypeImpl;
import grakn.core.concept.schema.impl.EntityTypeImpl;
import grakn.core.concept.schema.impl.RelationTypeImpl;
import grakn.core.concept.schema.impl.RuleImpl;
import grakn.core.concept.schema.impl.ThingTypeImpl;
import grakn.core.concept.schema.impl.TypeImpl;
import grakn.core.graph.Graphs;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.RuleVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import graql.lang.pattern.Pattern;

import java.util.ArrayList;
import java.util.List;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Transaction.UNSUPPORTED_OPERATION;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ATTRIBUTE_VALUE_TYPE_MISSING;

public final class Concepts {

    private final Graphs graph;

    public Concepts(Graphs graph) {
        this.graph = graph;
    }

    public ThingType getRootThingType() {
        TypeVertex vertex = graph.schema().getType(Encoding.Vertex.Type.Root.THING.label());
        if (vertex != null) return new ThingTypeImpl.Root(vertex);
        else throw graph.exception(ILLEGAL_STATE.message());
    }

    public EntityType getRootEntityType() {
        TypeVertex vertex = graph.schema().getType(Encoding.Vertex.Type.Root.ENTITY.label());
        if (vertex != null) return EntityTypeImpl.of(vertex);
        else throw graph.exception(ILLEGAL_STATE.message());
    }

    public RelationType getRootRelationType() {
        TypeVertex vertex = graph.schema().getType(Encoding.Vertex.Type.Root.RELATION.label());
        if (vertex != null) return RelationTypeImpl.of(vertex);
        else throw graph.exception(ILLEGAL_STATE.message());
    }

    public AttributeType getRootAttributeType() {
        TypeVertex vertex = graph.schema().getType(Encoding.Vertex.Type.Root.ATTRIBUTE.label());
        if (vertex != null) return AttributeTypeImpl.of(vertex);
        else throw graph.exception(ILLEGAL_STATE.message());
    }

    public EntityType putEntityType(String label) {
        TypeVertex vertex = graph.schema().getType(label);
        if (vertex != null) return EntityTypeImpl.of(vertex);
        else return EntityTypeImpl.of(graph.schema(), label);
    }

    public EntityType getEntityType(String label) {
        TypeVertex vertex = graph.schema().getType(label);
        if (vertex != null) return EntityTypeImpl.of(vertex);
        else return null;
    }

    public RelationType putRelationType(String label) {
        TypeVertex vertex = graph.schema().getType(label);
        if (vertex != null) return RelationTypeImpl.of(vertex);
        else return RelationTypeImpl.of(graph.schema(), label);
    }

    public RelationType getRelationType(String label) {
        TypeVertex vertex = graph.schema().getType(label);
        if (vertex != null) return RelationTypeImpl.of(vertex);
        else return null;
    }

    public AttributeType putAttributeType(String label, AttributeType.ValueType valueType) {
        if (valueType == null) throw graph.exception(ATTRIBUTE_VALUE_TYPE_MISSING.message(label));
        if (!valueType.isWritable()) throw graph.exception(UNSUPPORTED_OPERATION.message());

        TypeVertex vertex = graph.schema().getType(label);
        switch (valueType) {
            case BOOLEAN:
                if (vertex != null) return AttributeTypeImpl.Boolean.of(vertex);
                else return new AttributeTypeImpl.Boolean(graph.schema(), label);
            case LONG:
                if (vertex != null) return AttributeTypeImpl.Long.of(vertex);
                else return new AttributeTypeImpl.Long(graph.schema(), label);
            case DOUBLE:
                if (vertex != null) return AttributeTypeImpl.Double.of(vertex);
                else return new AttributeTypeImpl.Double(graph.schema(), label);
            case STRING:
                if (vertex != null) return AttributeTypeImpl.String.of(vertex);
                else return new AttributeTypeImpl.String(graph.schema(), label);
            case DATETIME:
                if (vertex != null) return AttributeTypeImpl.DateTime.of(vertex);
                else return new AttributeTypeImpl.DateTime(graph.schema(), label);
            default:
                throw graph.exception(UNSUPPORTED_OPERATION.message("putAttributeType", valueType.name()));
        }
    }

    public AttributeType getAttributeType(String label) {
        TypeVertex vertex = graph.schema().getType(label);
        if (vertex != null) return AttributeTypeImpl.of(vertex);
        else return null;
    }

    public Type getType(String label) {
        final TypeVertex vertex = graph.schema().getType(label);
        if (vertex != null) return TypeImpl.of(vertex);
        else return null;
    }

    public Rule putRule(String label, Pattern when, Pattern then) {
        RuleVertex vertex = graph.schema().getRule(label);
        if (vertex != null) {
            Rule rule = RuleImpl.of(vertex);
            rule.setWhen(when);
            rule.setThen(then);
            return rule;
        } else {
            return RuleImpl.of(graph.schema(), label, when, then);
        }
    }

    public Rule getRule(String label) {
        RuleVertex ruleVertex = graph.schema().getRule(label);
        if (ruleVertex != null) return RuleImpl.of(ruleVertex);
        return null;
    }

    public Thing getThing(final byte[] iid) {
        final ThingVertex thingVertex = graph.data().get(VertexIID.Thing.of(iid));
        if (thingVertex != null) return ThingImpl.of(thingVertex);
        else return null;
    }

    public void validateTypes() {
        List<GraknException> exceptions = graph.schema().types().parallel()
                .filter(Vertex::isModified)
                .map(v -> TypeImpl.of(v).validate())
                .collect(ArrayList::new, ArrayList::addAll, ArrayList::addAll);
        if (!exceptions.isEmpty()) throw graph.exception(GraknException.getMessages(exceptions));
    }

    public void validateThings() {
        graph.data().vertices().parallel()
                .filter(v -> !v.isInferred() && v.isModified() && !v.encoding().equals(Encoding.Vertex.Thing.ROLE))
                .forEach(v -> ThingImpl.of(v).validate());
    }

    public GraknException exception(String errorMessage) {
        return graph.exception(errorMessage);
    }
}
