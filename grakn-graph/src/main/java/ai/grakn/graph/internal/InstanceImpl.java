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
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *     A data instance in the graph belonging to a specific {@link Type}
 * </p>
 *
 * <p>
 *     Instances represent data in the graph.
 *     Every instance belongs to a {@link Type} which serves as a way of categorising them.
 *     Instances can relate to one another via {@link Relation}
 * </p>
 *
 * @author fppt
 *
 * @param <T> The leaf interface of the object concept which extends {@link Instance}.
 *           For example {@link ai.grakn.concept.Entity} or {@link Relation}.
 * @param <V> The type of the concept which extends {@link Type} of the concept.
 *           For example {@link ai.grakn.concept.EntityType} or {@link RelationType}
 */
abstract class InstanceImpl<T extends Instance, V extends Type> extends ConceptImpl<T> implements Instance {
    private ComponentCache<TypeName> cachedInternalType = new ComponentCache<>(() -> TypeName.of(getProperty(Schema.ConceptProperty.TYPE)));
    private ComponentCache<V> cachedType = new ComponentCache<>(() -> this.<V>getOutgoingNeighbours(Schema.EdgeLabel.ISA).findFirst().orElse(null));

    InstanceImpl(AbstractGraknGraph graknGraph, Vertex v) {
        super(graknGraph, v);
    }

    InstanceImpl(AbstractGraknGraph graknGraph, Vertex v, V type) {
        super(graknGraph, v);
        type(type);
    }

    /**
     * Deletes the concept as an Instance
     */
    @Override
    public void delete() {
        InstanceImpl<?, ?> parent = this;
        Set<CastingImpl> castings = parent.castings();
        deleteNode();
        for(CastingImpl casting: castings){
            Set<Relation> relations = casting.getRelations();
            getGraknGraph().getConceptLog().trackConceptForValidation(casting);

            for(Relation relation : relations) {
                if(relation.type().isImplicit()){//For now implicit relations die
                    relation.delete();
                } else {
                    RelationImpl rel = (RelationImpl) relation;
                    getGraknGraph().getConceptLog().trackConceptForValidation(rel);
                    rel.cleanUp();
                }
            }

            casting.deleteNode();
        }
    }

    /**
     * This index is used by concepts such as casting and relations to speed up internal lookups
     * @return The inner index value of some concepts.
     */
    public String getIndex(){
        return getProperty(Schema.ConceptProperty.INDEX);
    }

    /**
     *
     * @return All the {@link Resource} that this Instance is linked with
     */
    public Collection<Resource<?>> resources(ResourceType... resourceTypes) {
        Set<ConceptId> resourceTypesIds = Arrays.stream(resourceTypes).map(Concept::getId).collect(Collectors.toSet());

        Set<Resource<?>> resources = new HashSet<>();
        this.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).forEach(concept -> {
            if(concept.isResource()) {
                Resource<?> resource = concept.asResource();
                if(resourceTypesIds.isEmpty() || resourceTypesIds.contains(resource.type().getId())) {
                    resources.add(resource);
                }
            }
        });
        return resources;
    }

    /**
     *
     * @return All the {@link CastingImpl} that this Instance is linked with
     */
    public Set<CastingImpl> castings(){
        Set<CastingImpl> castings = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.ROLE_PLAYER).forEach(casting -> castings.add((CastingImpl) casting));
        return castings;
    }

    /**
     *
     * @param roleTypes An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    @Override
    public Collection<Relation> relations(RoleType... roleTypes) {
        Set<Relation> relations = new HashSet<>();
        Set<TypeName> roleTypeNames = Arrays.stream(roleTypes).map(RoleType::getName).collect(Collectors.toSet());

        InstanceImpl<?, ?> parent = this;

        parent.castings().forEach(c -> {
            CastingImpl casting = c.asCasting();
            if (roleTypeNames.size() != 0) {
                if (roleTypeNames.contains(casting.getInternalType())) {
                    relations.addAll(casting.getRelations());
                }
            } else {
                relations.addAll(casting.getRelations());
            }
        });

        return relations;
    }

    /**
     *
     * @return A set of all the Role Types which this instance plays.
     */
    @Override
    public Collection<RoleType> playsRoles() {
        Set<RoleType> roleTypes = new HashSet<>();
        ConceptImpl<?> parent = this;
        parent.getIncomingNeighbours(Schema.EdgeLabel.ROLE_PLAYER).forEach(c -> roleTypes.add(((CastingImpl)c).getRole()));
        return roleTypes;
    }


    /**
     * Creates a relation from this instance to the provided resource.
     * @param resource The resource to creating a relationship to
     * @return The instance itself
     */
    @Override
    public T hasResource(Resource resource){
        return resource(resource, Schema.ImplicitType.HAS_RESOURCE, Schema.ImplicitType.HAS_RESOURCE_VALUE, Schema.ImplicitType.HAS_RESOURCE_OWNER);
    }

    @Override
    public T key(Resource resource){
        return resource(resource, Schema.ImplicitType.HAS_KEY, Schema.ImplicitType.HAS_KEY_VALUE, Schema.ImplicitType.HAS_KEY_OWNER);
    }

    private T resource(Resource resource, Schema.ImplicitType has, Schema.ImplicitType hasValue, Schema.ImplicitType hasOwner){
        TypeName name = resource.type().getName();
        RelationType hasResource = getGraknGraph().getType(has.getName(name));
        RoleType hasResourceTarget = getGraknGraph().getType(hasOwner.getName(name));
        RoleType hasResourceValue = getGraknGraph().getType(hasValue.getName(name));

        if(hasResource == null || hasResourceTarget == null || hasResourceValue == null){
            String type;
            if(Schema.ImplicitType.HAS_KEY.equals(has)){
                type = "key";
            } else {
                type = "resource";
            }
            throw new ConceptException(ErrorMessage.HAS_INVALID.getMessage(type().getName(), type, resource.type().getName()));
        }

        Relation relation = hasResource.addRelation();
        relation.putRolePlayer(hasResourceTarget, this);
        relation.putRolePlayer(hasResourceValue, resource);

        return getThis();
    }

    /**
     *
     * @return The type of the concept casted to the correct interface
     */
    public V type() {
        return cachedType.get();
    }

    /**
     *
     * @param type The type of this concept
     * @return The concept itself casted to the correct interface
     */
    protected T type(V type) {
        if(type != null){
            setInternalType(type.getName());
            putEdge(type, Schema.EdgeLabel.ISA);
            cachedType.set(type);
        }
        return getThis();
    }

    /**
     *
     * @param type The type of this concept
     * @return The concept itself casted to the correct interface
     */
    private T setInternalType(TypeName type){
        cachedInternalType.set(type);
        return setProperty(Schema.ConceptProperty.TYPE, type.getValue());
    }

    /**
     *
     * @return The id of the type of this concept. This is a shortcut used to prevent traversals.
     */
    public TypeName getInternalType(){
        return cachedInternalType.get();
    }
}
