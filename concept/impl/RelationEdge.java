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

package grakn.core.concept.impl;

import grakn.core.concept.cache.ConceptCache;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.RelationStructure;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.VertexElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Encapsulates The Relation as a EdgeElement
 * This wraps up a Relation as a EdgeElement. It is used to represent any binary Relation.
 * This also includes the ability to automatically reify a RelationEdge into a RelationReified.
 */
public class RelationEdge implements RelationStructure {
    private final Logger LOG = LoggerFactory.getLogger(RelationEdge.class);

    private final EdgeElement edgeElement;
    private final ConceptManager conceptManager;
    private final ConceptNotificationChannel conceptNotificationChannel;

    private final ConceptCache<RelationType> relationType = new ConceptCache<>(() ->
            conceptManager().getSchemaConcept(LabelId.of(edge().property(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID))));

    private final ConceptCache<Role> ownerRole = new ConceptCache<>(() -> conceptManager().getSchemaConcept(LabelId.of(
            edge().property(Schema.EdgeProperty.RELATION_ROLE_OWNER_LABEL_ID))));

    private final ConceptCache<Role> valueRole = new ConceptCache<>(() -> conceptManager().getSchemaConcept(LabelId.of(
            edge().property(Schema.EdgeProperty.RELATION_ROLE_VALUE_LABEL_ID))));

    private final ConceptCache<Thing> owner = new ConceptCache<>(() -> conceptManager().buildConcept(edge().source()));
    private final ConceptCache<Thing> value = new ConceptCache<>(() -> conceptManager().buildConcept(edge().target()));

    public RelationEdge(EdgeElement edgeElement, ConceptManager conceptManager, ConceptNotificationChannel conceptNotificationChannel) {
        this.edgeElement = edgeElement;
        this.conceptManager = conceptManager;
        this.conceptNotificationChannel = conceptNotificationChannel;
    }

    private EdgeElement edge() {
        return edgeElement;
    }

    private ConceptManager conceptManager() {
        return conceptManager;
    }

    @Override
    public ConceptId id() {
        return Schema.conceptId(edge().element());
    }

    @Override
    public RelationReified reify() {
        LOG.debug("Reifying concept [{}]", id());
        //Build the Relation Vertex
        VertexElement relationVertex = edge().asReifiedVertexElement(isInferred());

        // safe downcast from interface to reified relation implementation
        RelationReified relationReified = (RelationReified) conceptManager().createRelationReified(relationVertex, type());

        //Delete the old edge
        delete();

        return relationReified;
    }

    @Override
    public boolean isReified() {
        return false;
    }

    @Override
    public RelationType type() {
        return relationType.get();
    }

    @Override
    public Map<Role, Set<Thing>> allRolePlayers() {
        HashMap<Role, Set<Thing>> result = new HashMap<>();
        result.put(ownerRole(), Collections.singleton(owner()));
        result.put(valueRole(), Collections.singleton(value()));
        return result;
    }

    @Override
    public Stream<Thing> rolePlayers(Role... roles) {
        if (roles.length == 0) {
            return Stream.of(owner(), value());
        }

        HashSet<Thing> result = new HashSet<>();
        for (Role role : roles) {
            if (role.equals(ownerRole())) {
                result.add(owner());
            } else if (role.equals(valueRole())) {
                result.add(value());
            }
        }
        return result.stream().filter(thing -> !this.isDeleted());
    }

    private Role ownerRole() {
        return ownerRole.get();
    }

    public Thing owner() {
        return owner.get();
    }

    private Role valueRole() {
        return valueRole.get();
    }

    public Thing value() {
        return value.get();
    }

    @Override
    public void delete() {
        if (!isDeleted()) {
            Supplier<Concept> conceptRetriever = () -> conceptManager.getConcept(id());
            conceptNotificationChannel.relationEdgeDeleted(this.type(), this.isInferred(), conceptRetriever);
            edge().delete();
        }
    }

    @Override
    public boolean isDeleted() {
        return edgeElement.isDeleted();
    }

    @Override
    public boolean isInferred() {
        return edge().propertyBoolean(Schema.EdgeProperty.IS_INFERRED);
    }

    @Override
    public String toString() {
        return "ID [" + id() + "] Type [" + type().label() + "] Roles and Role Players: \n" +
                "Role [" + ownerRole().label() + "] played by [" + owner().id() + "] \n" +
                "Role [" + valueRole().label() + "] played by [" + value().id() + "] \n";
    }
}
