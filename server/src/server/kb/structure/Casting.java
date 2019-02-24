/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.server.kb.structure;

import grakn.core.concept.LabelId;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.thing.Thing;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.cache.Cache;
import grakn.core.server.kb.cache.CacheOwner;
import grakn.core.server.kb.cache.Cacheable;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents An Thing Playing a Role
 * Wraps the Schema.EdgeLabel#ROLE_PLAYER Edge which contains the information unifying an Thing,
 * Relation and Role.
 */
public class Casting implements CacheOwner {
    private final Set<Cache> registeredCaches = new HashSet<>();
    private final EdgeElement edgeElement;

    private final Cache<Role> cachedRole = Cache.createTxCache(this, Cacheable.concept(), () ->
            edge().tx().getSchemaConcept(LabelId.of(edge().property(Schema.EdgeProperty.ROLE_LABEL_ID))));
    private final Cache<Thing> cachedInstance = Cache.createTxCache(this, Cacheable.concept(), () ->
            edge().tx().factory().<Thing>buildConcept(edge().target()));
    private final Cache<Relation> cachedRelationship = Cache.createTxCache(this, Cacheable.concept(), () ->
            edge().tx().factory().<Thing>buildConcept(edge().source()));

    private final Cache<RelationType> cachedRelationshipType = Cache.createTxCache(this, Cacheable.concept(), () -> {
        if (cachedRelationship.isPresent()) {
            return cachedRelationship.get().type();
        } else {
            return edge().tx().getSchemaConcept(LabelId.of(edge().property(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID)));
        }
    });

    private Casting(EdgeElement edgeElement, @Nullable Relation relationship, @Nullable Role role, @Nullable Thing thing) {
        this.edgeElement = edgeElement;
        if (relationship != null) this.cachedRelationship.set(relationship);
        if (role != null) this.cachedRole.set(role);
        if (thing != null) this.cachedInstance.set(thing);
    }

    public static Casting create(EdgeElement edgeElement, Relation relationship, Role role, Thing thing) {
        return new Casting(edgeElement, relationship, role, thing);
    }

    public static Casting withThing(EdgeElement edgeElement, Thing thing) {
        return new Casting(edgeElement, null, null, thing);
    }

    public static Casting withRelationship(EdgeElement edgeElement, Relation relationship) {
        return new Casting(edgeElement, relationship, null, null);
    }

    private EdgeElement edge() {
        return edgeElement;
    }

    @Override
    public Collection<Cache> caches() {
        return registeredCaches;
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
    public RelationType getRelationshipType() {
        return cachedRelationshipType.get();
    }

    /**
     * @return The Relation which is linking the Role and the instance
     */
    public Relation getRelationship() {
        return cachedRelationship.get();
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

        Casting casting = (Casting) object;

        return edge().id().equals(casting.edge().id());
    }
}
