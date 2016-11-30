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
import ai.grakn.exception.ConceptNotUniqueException;
import ai.grakn.util.Schema;
import ai.grakn.util.ErrorMessage;
import ai.grakn.exception.ConceptException;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RoleType;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * A relation represents and instance of a relation type which concept how different entities relate to one another.
 */
class RelationImpl extends InstanceImpl<Relation, RelationType> implements Relation {
    RelationImpl(Vertex v, RelationType type, AbstractGraknGraph graknGraph) {
        super(v, type, graknGraph);
    }

    /**
     *
     * @return All the castings this relation is connected with
     */
    public Set<CastingImpl> getMappingCasting() {
        Set<CastingImpl> castings = new HashSet<>();
        getOutgoingNeighbours(Schema.EdgeLabel.CASTING).forEach(casting -> castings.add(((CastingImpl) casting)));
        return castings;
    }

    /**
     * Sets the internal hash in order to perform a faster lookup
     */
    public void setHash(){
        setUniqueProperty(Schema.ConceptProperty.INDEX, generateNewHash(type(), rolePlayers()));
    }

    /**
     *
     * @param relationType The type of this relation
     * @param roleMap The roles and their corresponding role players
     * @return A unique hash identifying this relation
     */
    public static String generateNewHash(RelationType relationType, Map<RoleType, Instance> roleMap){
        SortedSet<RoleType> sortedRoleIds = new TreeSet<>(roleMap.keySet());
        String hash = "RelationType_" + relationType.getId().replace("_", "\\_") + "_Relation";

        for(RoleType role: sortedRoleIds){
            hash = hash + "_" + role.getId().replace("_", "\\_") ;
            Instance instance = roleMap.get(role);
            if(instance != null){
                hash = hash + "_" + instance.getId().replace("_", "\\_") ;
            }
        }
        return hash;
    }

    /**
     *
     * @return A list of all the Instances involved in the relationships and the Role Types which they play.
     */
    @Override
    public Map<RoleType, Instance> rolePlayers() {
        Set<CastingImpl> castings = getMappingCasting();
        HashMap<RoleType, Instance> roleMap = new HashMap<>();

        //Gets roles based on all roles of the relation type
        type().hasRoles().forEach(roleType -> roleMap.put(roleType, null));

        //Get roles based on availiable castings
        castings.forEach(casting -> roleMap.put(casting.getRole(), casting.getRolePlayer()));

        return roleMap;
    }

    /**
     *
     * @return A list of the Instances which scope this Relation
     */
    @Override
    public Set<Instance> scopes() {
        HashSet<Instance> scopes = new HashSet<>();
        getOutgoingNeighbours(Schema.EdgeLabel.HAS_SCOPE).forEach(concept -> scopes.add(concept.asInstance()));
        return scopes;
    }

    /**
     *
     * @param instance A new instance which can scope this Relation
     * @return The Relation itself
     */
    @Override
    public Relation scope(Instance instance) {
        putEdge(instance, Schema.EdgeLabel.HAS_SCOPE);
        return this;
    }

    /**
     * Expands this Relation to include a new role player which is playing a specific role.
     * @param roleType The role of the new role player.
     * @param instance The new role player.
     * @return The Relation itself
     */
    @Override
    public Relation putRolePlayer(RoleType roleType, Instance instance) {
        if(roleType == null){
            throw new IllegalArgumentException(ErrorMessage.ROLE_IS_NULL.getMessage(instance));
        }

        //Check if it is a unique resource
        if(instance != null && instance.isResource()){
            Resource<Object> resource = instance.asResource();
            if(resource.type().isUnique()) {

                GraphTraversal traversal = getGraknGraph().getTinkerTraversal().
                        hasId(resource.getId()).
                        out(Schema.EdgeLabel.SHORTCUT.getLabel());

                if(traversal.hasNext()) {
                    ConceptImpl foundNeighbour = getGraknGraph().getElementFactory().buildUnknownConcept((Vertex) traversal.next());
                    throw new ConceptNotUniqueException(resource, foundNeighbour.asInstance());
                }
            }
        }

        //Do the actual put of the role and role player
        return addNewRolePlayer(roleType, instance);
    }

    /**
     * Adds a new role player to this relation
     * @param roleType The role of the new role player.
     * @param instance The new role player.
     * @return The Relation itself
     */
    private Relation addNewRolePlayer(RoleType roleType, Instance instance){
        if(instance != null)
            getGraknGraph().putCasting((RoleTypeImpl) roleType, (InstanceImpl) instance, this);
        return this;
    }

    /**
     * @param scope A concept which is currently scoping this concept.
     * @return The Relation itself
     */
    @Override
    public Relation deleteScope(Instance scope) throws ConceptException {
        deleteEdgeTo(Schema.EdgeLabel.HAS_SCOPE, scope);
        return this;
    }

    /**
     * When a relation is deleted this cleans up any solitary casting and resources.
     */
    public void cleanUp() {
        boolean performDeletion = true;
        Collection<Instance> rolePlayers = rolePlayers().values();

        // tracking
        rolePlayers.forEach(r -> {
            if(r != null)
                getGraknGraph().getConceptLog().putConcept((ConceptImpl) r);
        });
        this.getMappingCasting().forEach(c -> getGraknGraph().getConceptLog().putConcept(c));

        for(Instance instance : rolePlayers){
            if(instance != null && (instance.getId() != null )){
                performDeletion = false;
            }
        }

        if(performDeletion){
            delete();
        }
    }

    /**
     * Deletes the concept as a Relation
     */
    @Override
    public void innerDelete() {
        scopes().forEach(this::deleteScope);
        Set<CastingImpl> castings = getMappingCasting();

        for (CastingImpl casting: castings) {
            InstanceImpl<?, ?> instance = casting.getRolePlayer();
            if(instance != null) {
                for (EdgeImpl edge : instance.getEdgesOfType(Direction.BOTH, Schema.EdgeLabel.SHORTCUT)) {
                    if(edge.getProperty(Schema.EdgeProperty.RELATION_ID).equals(getId())){
                        edge.delete();
                    }
                }
            }
        }

        super.innerDelete();
    }

    @Override
    public String toString(){
        String description = "ID [" + getId() +  "] Type [" + type().getName() + "] Roles and Role Players: \n";
        for (Map.Entry<RoleType, Instance> entry : rolePlayers().entrySet()) {
            if(entry.getValue() == null){
                description += "    Role [" + entry.getKey().getName() + "] not played by any instance \n";
            } else {
                description += "    Role [" + entry.getKey().getName() + "] played by [" + entry.getValue().getId() + "] \n";
            }
        }
        return description;
    }
}
