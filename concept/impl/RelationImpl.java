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

import com.google.common.collect.Iterables;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationStructure;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.structure.VertexElement;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Encapsulates relations between Thing
 * A relation which is an instance of a RelationType defines how instances may relate to one another.
 */
public class RelationImpl implements Relation, ConceptVertex {
    private RelationStructure relationStructure;

    public RelationImpl(RelationStructure relationStructure) {
        this.relationStructure = relationStructure;
        if (relationStructure.isReified()) {
            ((RelationReified)relationStructure.reify()).owner(this);
        }
    }

    public static RelationImpl from(Relation relation) {
        return (RelationImpl) relation;
    }

    /**
     * Gets the RelationReified if the Relation has been reified.
     * To reify the Relation you use RelationImpl.reify().
     * NOTE: This approach is done to make sure that only write operations will cause the Relation to reify
     *
     * @return The RelationReified if the Relation has been reified
     */
    @Nullable
    public RelationReified reified() {
        if (relationStructure.isReified()) return (RelationReified)relationStructure.reify();
        return null;
    }

    /**
     * Reifies and returns the RelationReified
     */
    private RelationReified reify() {
        if (relationStructure.isReified()) return (RelationReified) relationStructure.reify();

        //Get the role players to transfer
        Map<Role, Set<Thing>> rolePlayers = structure().allRolePlayers();

        //Now Reify
        relationStructure = relationStructure.reify();

        //Transfer relations
        rolePlayers.forEach((role, things) -> {
            Thing thing = Iterables.getOnlyElement(things);
            assign(role, thing);
        });

        return (RelationReified) relationStructure.reify();
    }

    public RelationStructure structure() {
        return relationStructure;
    }

    @Override
    public Relation has(Attribute attribute) {
        relhas(attribute);
        return this;
    }

    @Override
    public Relation relhas(Attribute attribute) {
        return reify().relhas(attribute);
    }

    @Override
    public Stream<Attribute<?>> attributes(AttributeType[] attributeTypes) {
        return readFromReified((relationReified) -> relationReified.attributes(attributeTypes));
    }

    @Override
    public Stream<Attribute<?>> keys(AttributeType[] attributeTypes) {
        RelationReified relationReified = reified();
        return relationReified != null? relationReified.attributes(attributeTypes) : Stream.empty();
    }

    @Override
    public RelationType type() {
        return structure().type();
    }

    @Override
    public Stream<Relation> relations(Role... roles) {
        return readFromReified((relationReified) -> relationReified.relations(roles));
    }

    @Override
    public Stream<Role> roles() {
        return readFromReified(ThingImpl::roles);
    }

    /**
     * Reads some data from a RelationReified. If the Relation has not been reified then an empty
     * Stream is returned.
     */
    private <X> Stream<X> readFromReified(Function<RelationReified, Stream<X>> producer) {
        RelationReified relationReified = reified();
        return relationReified != null? producer.apply(relationReified) : Stream.empty();
    }

    /**
     * Retrieve a list of all Thing involved in the Relation, and the Role they play.
     *
     * @return A list of all the Roles and the Things playing them in this Relation.
     * see Role
     */
    @Override
    public Map<Role, Set<Thing>> rolePlayersMap() {
        return structure().allRolePlayers();
    }

    @Override
    public Stream<Thing> rolePlayers(Role... roles) {
        return structure().rolePlayers(roles);
    }

    /**
     * Expands this Relation to include a new role player which is playing a specific Role.
     *
     * @param role   The role of the new role player.
     * @param player The new role player.
     * @return The Relation itself
     */
    @Override
    public Relation assign(Role role, Thing player) {
        reify().addRolePlayer(role, player);
        return this;
    }

    @Override
    public Relation unhas(Attribute attribute) {
        RelationReified relationReified = reified();
        if (relationReified != null) relationReified.unhas(attribute);
        return this;
    }

    @Override
    public boolean isInferred() {
        return structure().isInferred();
    }

    @Override
    public void unassign(Role role, Thing player) {
        RelationReified relationReified = reified();
        if (relationReified != null){
            relationReified.removeRolePlayer(role, player);
        }
    }

    /**
     * When a relation is deleted this cleans up any solitary casting and resources.
     */
    void cleanUp() {
        Stream<Thing> rolePlayers = rolePlayers();
        boolean performDeletion = rolePlayers.noneMatch(thing -> thing != null && thing.id() != null);
        if (performDeletion) delete();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        return id().equals(((RelationImpl) object).id());
    }

    @Override
    public int hashCode() {
        return id().hashCode();
    }

    @Override
    public String toString() {
        return structure().toString();
    }

    @Override
    public ConceptId id() {
        return structure().id();
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

    @Override
    public Relation attributeInferred(Attribute attribute) {
        return reify().attributeInferred(attribute);
    }

    @Override
    public Stream<Thing> getDependentConcepts() {
        return Stream.concat(Stream.of(this), rolePlayers());
    }
}
