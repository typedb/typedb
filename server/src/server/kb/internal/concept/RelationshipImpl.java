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

package grakn.core.server.kb.internal.concept;

import grakn.core.server.Keyspace;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Relationship;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.server.kb.internal.cache.Cache;
import grakn.core.server.kb.internal.cache.CacheOwner;
import grakn.core.server.kb.internal.structure.VertexElement;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * <p>
 *     Encapsulates relationships between {@link Thing}
 * </p>
 *
 * <p>
 *     A relation which is an instance of a {@link RelationshipType} defines how instances may relate to one another.
 * </p>
 *
 *
 */
public class RelationshipImpl implements Relationship, ConceptVertex, CacheOwner {
    private RelationshipStructure relationshipStructure;

    private RelationshipImpl(RelationshipStructure relationshipStructure) {
        this.relationshipStructure = relationshipStructure;
        if(relationshipStructure.isReified()){
            relationshipStructure.reify().owner(this);
        }
    }

    public static RelationshipImpl create(RelationshipStructure relationshipStructure) {
        return new RelationshipImpl(relationshipStructure);
    }

    /**
     * Gets the {@link RelationshipReified} if the {@link Relationship} has been reified.
     * To reify the {@link Relationship} you use {@link RelationshipImpl#reify()}.
     *
     * NOTE: This approach is done to make sure that only write operations will cause the {@link Relationship} to reify
     *
     * @return The {@link RelationshipReified} if the {@link Relationship} has been reified
     */
    public Optional<RelationshipReified> reified(){
        if(!relationshipStructure.isReified()) return Optional.empty();
        return Optional.of(relationshipStructure.reify());
    }

    /**
     * Reifys and returns the {@link RelationshipReified}
     */
    public RelationshipReified reify(){
        if(relationshipStructure.isReified()) return relationshipStructure.reify();

        //Get the role players to transfer
        Map<Role, Set<Thing>> rolePlayers = structure().allRolePlayers();

        //Now Reify
        relationshipStructure = relationshipStructure.reify();

        //Transfer relationships
        rolePlayers.forEach((role, things) -> {
            Thing thing = Iterables.getOnlyElement(things);
            assign(role, thing);
        });

        return relationshipStructure.reify();
    }

    public RelationshipStructure structure(){
        return relationshipStructure;
    }

    @Override
    public Relationship has(Attribute attribute) {
        relhas(attribute);
        return this;
    }

    @Override
    public Relationship relhas(Attribute attribute) {
        return reify().relhas(attribute);
    }

    @Override
    public Stream<Attribute<?>> attributes(AttributeType[] attributeTypes) {
        return readFromReified((relationReified) -> relationReified.attributes(attributeTypes));
    }

    @Override
    public Stream<Attribute<?>> keys(AttributeType[] attributeTypes) {
        return reified().map(relationshipReified -> relationshipReified.attributes(attributeTypes)).orElseGet(Stream::empty);
    }

    @Override
    public RelationshipType type() {
        return structure().type();
    }

    @Override
    public Stream<Relationship> relationships(Role... roles) {
        return readFromReified((relationReified) -> relationReified.relationships(roles));
    }

    @Override
    public Stream<Role> roles() {
        return readFromReified(ThingImpl::roles);
    }

    /**
     * Reads some data from a {@link RelationshipReified}. If the {@link Relationship} has not been reified then an empty
     * {@link Stream} is returned.
     */
    private <X> Stream<X> readFromReified(Function<RelationshipReified, Stream<X>> producer){
        return reified().map(producer).orElseGet(Stream::empty);
    }

    /**
     * Retrieve a list of all {@link Thing} involved in the {@link Relationship}, and the {@link Role} they play.
     * @see Role
     *
     * @return A list of all the {@link Role}s and the {@link Thing}s playing them in this {@link Relationship}.
     */
    @Override
    public Map<Role, Set<Thing>> rolePlayersMap(){
       return structure().allRolePlayers();
    }

    @Override
    public Stream<Thing> rolePlayers(Role... roles) {
        return structure().rolePlayers(roles);
    }

    /**
     * Expands this {@link Relationship} to include a new role player which is playing a specific {@link Role}.
     * @param role The role of the new role player.
     * @param player The new role player.
     * @return The {@link Relationship} itself
     */
    @Override
    public Relationship assign(Role role, Thing player) {
        reify().addRolePlayer(role, player);
        return this;
    }

    @Override
    public Relationship unhas(Attribute attribute) {
        reified().ifPresent(rel -> rel.unhas(attribute));
        return this;
    }

    @Override
    public boolean isInferred() {
        return structure().isInferred();
    }

    @Override
    public void unassign(Role role, Thing player) {
        reified().ifPresent(relationshipReified -> relationshipReified.removeRolePlayer(role, player));
    }

    /**
     * When a relation is deleted this cleans up any solitary casting and resources.
     */
    void cleanUp() {
        Stream<Thing> rolePlayers = rolePlayers();
        boolean performDeletion = rolePlayers.noneMatch(thing -> thing != null && thing.id() != null);
        if(performDeletion) delete();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        return id().equals(((RelationshipImpl) object).id());
    }

    @Override
    public int hashCode() {
        return id().hashCode();
    }

    @Override
    public String toString(){
        return structure().toString();
    }

    @Override
    public ConceptId id() {
        return structure().id();
    }

    @Override
    public Keyspace keyspace() {
        return structure().keyspace();
    }

    @Override
    public void delete() {
        structure().delete();
    }

    @Override
    public boolean isDeleted() {
        return structure().isDeleted();
    }

    @Override
    public VertexElement vertex() {
        return reify().vertex();
    }

    public static RelationshipImpl from(Relationship relationship){
        return (RelationshipImpl) relationship;
    }

    @Override
    public Collection<Cache> caches() {
        return structure().caches();
    }

    public Relationship attributeInferred(Attribute attribute) {
        reify().attributeInferred(attribute);
        return this;
    }
}
