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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.kb.internal.structure;

import ai.grakn.concept.LabelId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.kb.internal.cache.Cache;
import ai.grakn.kb.internal.cache.CacheOwner;
import ai.grakn.kb.internal.cache.Cacheable;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 *     Represents An Thing Playing a Role
 * </p>
 *
 * <p>
 *    Wraps the {@link Schema.EdgeLabel#ROLE_PLAYER} {@link Edge} which contains the information unifying an {@link Thing},
 *    {@link Relationship} and {@link Role}.
 * </p>
 *
 * @author fppt
 */
public class Casting implements CacheOwner{
    private final Set<Cache> registeredCaches = new HashSet<>();
    private final EdgeElement edgeElement;

    private final Cache<Role> cachedRole = Cache.createTxCache(this, Cacheable.concept(), () ->
            (Role) edge().tx().getSchemaConcept(LabelId.of(edge().property(Schema.EdgeProperty.ROLE_LABEL_ID))));
    private final Cache<Thing> cachedInstance = Cache.createTxCache(this, Cacheable.concept(), () ->
            edge().tx().factory().<Thing>buildConcept(edge().target()));
    private final Cache<Relationship> cachedRelationship = Cache.createTxCache(this, Cacheable.concept(), () ->
            edge().tx().factory().<Thing>buildConcept(edge().source()));

    private final Cache<RelationshipType> cachedRelationshipType = Cache.createTxCache(this, Cacheable.concept(), () -> {
        if(cachedRelationship.isPresent()){
            return cachedRelationship.get().type();
        } else {
            return (RelationshipType) edge().tx().getSchemaConcept(LabelId.of(edge().property(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID)));
        }
    });

    private Casting(EdgeElement edgeElement, @Nullable Relationship relationship, @Nullable Role role, @Nullable Thing thing){
        this.edgeElement = edgeElement;
        if(relationship != null) this.cachedRelationship.set(relationship);
        if(role != null) this.cachedRole.set(role);
        if(thing != null) this.cachedInstance.set(thing);
    }

    public static Casting create(EdgeElement edgeElement, Relationship relationship, Role role, Thing thing) {
        return new Casting(edgeElement, relationship, role, thing);
    }

    public static Casting withThing(EdgeElement edgeElement, Thing thing){
        return new Casting(edgeElement, null, null, thing);
    }

    public static Casting withRelationship(EdgeElement edgeElement, Relationship relationship) {
        return new Casting(edgeElement, relationship, null, null);
    }

    private EdgeElement edge(){
        return edgeElement;
    }

    @Override
    public Collection<Cache> caches(){
        return registeredCaches;
    }

    /**
     *
     * @return The {@link Role} the {@link Thing} is playing
     */
    public Role getRole(){
        return cachedRole.get();
    }

    /**
     *
     * @return The {@link RelationshipType} the {@link Thing} is taking part in
     */
    public RelationshipType getRelationshipType(){
        return cachedRelationshipType.get();
    }

    /**
     *
     * @return The {@link Relationship} which is linking the {@link Role} and the instance
     */
    public Relationship getRelationship(){
        return cachedRelationship.get();
    }

    /**
     *
     * @return The {@link Thing} playing the {@link Role}
     */
    public Thing getRolePlayer(){
        return cachedInstance.get();
    }

    /**
     *
     * @return The hash code of the underlying vertex
     */
    public int hashCode() {
        return edge().id().hashCode();
    }

    /**
     * Deletes this {@link Casting} effectively removing a {@link Thing} from playing a {@link Role} in a {@link Relationship}
     */
    public void delete(){
        edge().delete();
    }

    /**
     *
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
