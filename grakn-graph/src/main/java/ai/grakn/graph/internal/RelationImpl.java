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
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.util.Schema;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
        //TODO remove the call to super
        super(reifiedRelation.vertex());
        this.reifiedRelation = reifiedRelation;
    }

    ReifiedRelation reified(){
        return reifiedRelation;
    }

    /**
     * Sets the internal hash in order to perform a faster lookup
     */
    void setHash(){
        vertex().propertyUnique(Schema.VertexProperty.INDEX, generateNewHash(type(), allRolePlayers()));
    }

    /**
     *
     * @param relationType The type of this relation
     * @param roleMap The roles and their corresponding role players
     * @return A unique hash identifying this relation
     */
    static String generateNewHash(RelationType relationType, Map<Role, Set<Thing>> roleMap){
        SortedSet<Role> sortedRoleIds = new TreeSet<>(roleMap.keySet());
        StringBuilder hash = new StringBuilder();
        hash.append("RelationType_").append(relationType.getId().getValue().replace("_", "\\_")).append("_Relation");

        for(Role role: sortedRoleIds){
            hash.append("_").append(role.getId().getValue().replace("_", "\\_"));

            roleMap.get(role).forEach(instance -> {
                if(instance != null){
                    hash.append("_").append(instance.getId().getValue().replace("_", "\\_"));
                }
            });
        }
        return hash.toString();
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
        return getThis();
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
