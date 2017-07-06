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
class RelationImpl extends ThingImpl<Relation, RelationType> implements Relation {
    private ReifiedRelation reifiedRelation;

    RelationImpl(ReifiedRelation reifiedRelation) {
        super(reifiedRelation.vertex());
        this.reifiedRelation = reifiedRelation;
    }

    ReifiedRelation reified(){
        return reifiedRelation;
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
        return reified().type();
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
     * Retrieve a list of all Instances involved in the Relation, and the Role Types they play.
     * @see Role
     *
     * @return A list of all the role types and the instances playing them in this relation.
     */
    @Override
    public Map<Role, Set<Thing>> allRolePlayers(){
       return reified().allRolePlayers();
    }

    @Override
    public Collection<Thing> rolePlayers(Role... roles) {
        return reified().rolePlayers(roles);
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
    public String innerToString(){
        StringBuilder description = new StringBuilder();
        description.append("ID [").append(getId()).append("] Type [").append(type().getLabel()).append("] Roles and Role Players: \n");
        for (Map.Entry<Role, Set<Thing>> entry : allRolePlayers().entrySet()) {
            if(entry.getValue().isEmpty()){
                description.append("    Role [").append(entry.getKey().getLabel()).append("] not played by any instance \n");
            } else {
                StringBuilder instancesString = new StringBuilder();
                for (Thing thing : entry.getValue()) {
                    instancesString.append(thing.getId()).append(",");
                }
                description.append("    Role [").append(entry.getKey().getLabel()).append("] played by [").
                        append(instancesString.toString()).append("] \n");
            }
        }
        return description.toString();
    }
}
