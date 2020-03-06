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

package grakn.core.kb.concept.manager;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationStructure;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.Shard;
import grakn.core.kb.concept.structure.VertexElement;
import graql.lang.pattern.Pattern;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

public interface ConceptManager {

    <T extends Concept> T buildConcept(Vertex vertex);
    <T extends Concept> T buildConcept(Edge edge);
    <T extends Concept> T buildConcept(VertexElement vertex);
    Relation buildRelation(EdgeElement edge);


    <T extends Type> T getType(Label label);
    <T extends SchemaConcept> T getSchemaConcept(Label label);
    <T extends SchemaConcept> T getSchemaConcept(LabelId labelId);
    EntityType getEntityType(String label);
    RelationType getRelationType(String label);
    <V> AttributeType<V> getAttributeType(String attributeTypeLabel);
    Role getRole(String label);
    Rule getRule(String label);
    <D> Attribute<D> getCachedAttribute(String index);
    <D> Attribute<D> getAttribute(String index);

    <T extends Concept> T getConcept(Schema.VertexProperty vertexProperty, Object propertyValue);
    <T extends Concept> T getConcept(ConceptId conceptId);

    Type getMetaConcept();
    EntityType getMetaEntityType();
    RelationType getMetaRelationType();
    AttributeType getMetaAttributeType();
    Role getMetaRole();
    Rule getMetaRule();


    Relation createRelation(RelationType relationType, boolean isInferred);
    Entity createEntity(EntityType entityType, boolean isInferred);
    <D> Attribute<D> createAttribute(AttributeType<D> dAttributeType, D value, boolean isInferred);
    RelationType createImplicitRelationType(Label label);
    Role createImplicitRole(Label label);
    Relation createHasAttributeRelation(EdgeElement attributeEdge, RelationType hasAttribute, Role hasAttributeOwner,
                                        Role hasAttributeValue, boolean isInferred);

    // TODO this wants to return implementation RelationReified, not interface RelationStructure, using downcasts for now
    RelationStructure createRelationReified(VertexElement relationVertex, RelationType type);

    EntityType createEntityType(Label label, EntityType superType);
    RelationType createRelationType(Label label, RelationType superType);
    <V> AttributeType<V> createAttributeType(Label label, AttributeType<V> superType, AttributeType.DataType<V> dataType);
    Rule createRule(Label label, Pattern when, Pattern then, Rule superType);
    Role createRole(Label label, Role superType);

    Set<Concept> getConcepts(Schema.VertexProperty key, Object value);

    // TODO these should not be here, overexposed interface or incorrect location
    LabelId convertToId(Label label);
    VertexElement addTypeVertex(LabelId id, Label label, Schema.BaseType baseType);

    Shard getShardWithLock(String toString);
}
