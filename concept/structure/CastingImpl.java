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

package grakn.core.concept.structure;

import grakn.core.concept.cache.ConceptCache;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.structure.Casting;
import grakn.core.kb.concept.structure.EdgeElement;

import javax.annotation.Nullable;

/**
 * Represents An Thing Playing a Role
 * Wraps the Schema.EdgeLabel#ROLE_PLAYER Edge which contains the information unifying an Thing,
 * Relation and Role.
 */
public class CastingImpl implements Casting {

    private final EdgeElement edgeElement;
    private ConceptManager conceptManager;
    private final ConceptCache<Role> cachedRole = new ConceptCache<>(() -> conceptManager().getSchemaConcept(LabelId.of(edge().property(Schema.EdgeProperty.ROLE_LABEL_ID))).asRole());
    private final ConceptCache<Thing> cachedInstance = new ConceptCache<>(() -> conceptManager().buildConcept(edge().target()).asThing());
    private final ConceptCache<Relation> cachedRelation = new ConceptCache<>(() -> conceptManager().buildConcept(edge().source()).asRelation());

    private final ConceptCache<RelationType> cachedRelationType = new ConceptCache<>(() -> {
        if (cachedRelation.isCached()) {
            return cachedRelation.get().type();
        } else {
            return conceptManager().getRelationType(LabelId.of(edge().property(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID)).toString());
        }
    });

    CastingImpl(EdgeElement edgeElement, @Nullable Relation relation, @Nullable Role role, @Nullable Thing thing, ConceptManager conceptManager) {
        this.edgeElement = edgeElement;
        this.conceptManager = conceptManager;
        if (relation != null) this.cachedRelation.set(relation);
        if (role != null) this.cachedRole.set(role);
        if (thing != null) this.cachedInstance.set(thing);
    }

    public static Casting create(EdgeElement edgeElement, Relation relation, Role role, Thing thing, ConceptManager conceptManager) {
        return new CastingImpl(edgeElement, relation, role, thing, conceptManager);
    }

    public static Casting withThing(EdgeElement edgeElement, Thing thing, ConceptManager conceptManager) {
        return new CastingImpl(edgeElement, null, null, thing, conceptManager);
    }

    public static Casting withRelation(EdgeElement edgeElement, Relation relation, ConceptManager conceptManager) {
        return new CastingImpl(edgeElement, relation, null, null, conceptManager);
    }

    public EdgeElement edge() {
        return edgeElement;
    }

    private ConceptManager conceptManager() {
        return conceptManager;
    }

    /**
     * @return The Role the Thing is playing
     */
    public Role getRole() {
        return cachedRole.get();
    }

    /**
     * @return The RelationType the Thing is taking part in
     */
    public RelationType getRelationType() {
        return cachedRelationType.get();
    }

    /**
     * @return The Relation which is linking the Role and the instance
     */
    public Relation getRelation() {
        return cachedRelation.get();
    }

    /**
     * @return The Thing playing the Role
     */
    public Thing getRolePlayer() {
        return cachedInstance.get();
    }

    /**
     * @return The hash code of the underlying vertex
     */
    public int hashCode() {
        return edge().id().hashCode();
    }

    /**
     * Deletes this Casting effectively removing a Thing from playing a Role in a Relation
     */
    public void delete() {
        edge().delete();
    }

    /**
     * @return true if the elements equal each other
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        CastingImpl casting = (CastingImpl) object;

        return edge().id().equals(casting.edge().id());
    }
}
