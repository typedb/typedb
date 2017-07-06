/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graph.internal;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 *     Encapsulates relationships between {@link Thing}
 * </p>
 *
 * <p>
 *     A relation which is an instance of a {@link RelationType} defines how instances may relate to one another.
 * </p>
 *
 * @author fppt
 *
 */
class RelationImpl implements Relation, ConceptVertex {
    private RelationStructure relationStructure;

    RelationImpl(RelationStructure relationStructure) {
        this.relationStructure = relationStructure;
    }

    RelationReified reified(){
        if(!relationStructure.isReified()){
            throw new UnsupportedOperationException("Reification is not yet supported");
        }
        return relationStructure.reified();
    }

    RelationStructure structure(){
        return relationStructure;
    }

    @Override
    public Relation resource(Resource resource) {
        reified().resource(resource);
        return this;
    }

    @Override
    public Collection<Resource<?>> resources(ResourceType[] resourceTypes) {
        return reified().resources(resourceTypes);
    }

    @Override
    public RelationType type() {
        return structure().type();
    }

    @Override
    public Collection<Relation> relations(Role... roles) {
        return reified().relations();
    }

    @Override
    public Collection<Role> plays() {
        return reified().plays();
    }

    /**
     * Retrieve a list of all {@link Thing} involved in the {@link Relation}, and the {@link Role} they play.
     * @see Role
     *
     * @return A list of all the {@link Role}s and the {@link Thing}s playing them in this {@link Relation}.
     */
    @Override
    public Map<Role, Set<Thing>> allRolePlayers(){
       return structure().allRolePlayers();
    }

    @Override
    public Collection<Thing> rolePlayers(Role... roles) {
        return structure().rolePlayers(roles);
    }


    /**
     * Expands this Relation to include a new role player which is playing a specific role.
     * @param role The role of the new role player.
     * @param thing The new role player.
     * @return The Relation itself
     */
    @Override
    public Relation addRolePlayer(Role role, Thing thing) {
        reified().addRolePlayer(this, role, thing);
        return this;
    }

    /**
     * When a relation is deleted this cleans up any solitary casting and resources.
     */
    void cleanUp() {
        boolean performDeletion = true;
        Collection<Thing> rolePlayers = rolePlayers();

        for(Thing thing : rolePlayers){
            if(thing != null && (thing.getId() != null )){
                performDeletion = false;
            }
        }

        if(performDeletion){
            delete();
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        return getId().equals(((RelationImpl) object).getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString(){
        return structure().toString();
    }

    @Override
    public ConceptId getId() {
        return structure().getId();
    }

    @Override
    public void delete() {
        structure().delete();
    }

    @Override
    public int compareTo(Concept o) {
        return getId().compareTo(o.getId());
    }

    @Override
    public VertexElement vertex() {
        return reified().vertex();
    }
}
