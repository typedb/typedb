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

package ai.grakn.engine.controller.response;

import ai.grakn.exception.GraknBackendException;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *     Factory used to build the wrapper {@link Concept}s from real {@link ai.grakn.concept.Concept}s
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class ConceptBuilder {

    /**
     * Takes a {@link ai.grakn.concept.Concept} and returns the equivalent response object
     *
     * @param concept The {@link ai.grakn.concept.Concept} to be converted into a response object
     * @return the response object wrapper {@link Concept}
     */
    public static <X extends Concept> Optional<X> build(ai.grakn.Keyspace keyspace, ai.grakn.concept.Concept concept){
        if(concept == null) return Optional.empty();

        Concept response = null;

        if(concept.isSchemaConcept()){
            ai.grakn.concept.SchemaConcept schemaConcept = concept.asSchemaConcept();
            SchemaConcept sup = buildLinkOnlySchemaConcept(schemaConcept.sup());

            Set<SchemaConcept> subs = schemaConcept.subs().
                    map(ConceptBuilder::<SchemaConcept>buildLinkOnlySchemaConcept).
                    collect(Collectors.toSet());

            if(schemaConcept.isRole()){
                response = buildRole(schemaConcept.asRole(), sup, subs);
            } else if(schemaConcept.isRule()){
                response = buildRule(schemaConcept.asRule(), sup, subs);
            } else { //In this case it's a type
                ai.grakn.concept.Type type = schemaConcept.asType();

                Set<Role> rolesPlayed = type.plays().
                        map(ConceptBuilder::<Role>buildLinkOnlySchemaConcept).
                        collect(Collectors.toSet());

                Set<AttributeType> attributes = type.attributes().
                        map(ConceptBuilder::<AttributeType>buildLinkOnlyType).
                        collect(Collectors.toSet());

                Set<AttributeType> keys = type.keys().
                        map(ConceptBuilder::<AttributeType>buildLinkOnlyType).
                        collect(Collectors.toSet());


            }

        } else {
            throw GraknBackendException.convertingUnknownConcept(concept);
        }


        return Optional.of((X) response);
    }

    private static SchemaConcept buildSchemaConcept(ai.grakn.concept.SchemaConcept schemaConcept){
        SchemaConcept sup = buildLinkOnlySchemaConcept(schemaConcept.sup());

        Set<SchemaConcept> subs = schemaConcept.subs().
                map(ConceptBuilder::<SchemaConcept>buildLinkOnlySchemaConcept).
                collect(Collectors.toSet());

        if(schemaConcept.isRole()){
            return buildRole(schemaConcept.asRole(), sup, subs);
        } else if(schemaConcept.isRule()){
            return buildRule(schemaConcept.asRule(), sup, subs);
        } else {
            return buildType(schemaConcept.asType());
        }
    }

    private static Type buildType(ai.grakn.concept.Type type){
        Set<Role> rolesPlayed = type.plays().
                map(ConceptBuilder::<Role>buildLinkOnlySchemaConcept).
                collect(Collectors.toSet());

        Set<AttributeType> attributes = type.attributes().
                map(ConceptBuilder::<AttributeType>buildLinkOnlyType).
                collect(Collectors.toSet());

        Set<AttributeType> keys = type.keys().
                map(ConceptBuilder::<AttributeType>buildLinkOnlyType).
                collect(Collectors.toSet());

        if(type.isAttributeType()){

        } else if (type.isEntityType()){

        } else if (type.isRelationshipType()){

        } else {
            throw GraknBackendException.convertingUnknownConcept(type);
        }
    }

    private static Role buildRole(ai.grakn.concept.Role role, SchemaConcept sup, Set<SchemaConcept> subs){
        Set<RelationshipType> relationshipTypes = role.relationshipTypes().
                map(rel-> RelationshipType.createLinkOnly(rel.keyspace(), rel.getId(), rel.getLabel())).
                collect(Collectors.toSet());

        Set<Type> playedByTypes = role.playedByTypes().
                map(ConceptBuilder::<Type>buildLinkOnlyType).collect(Collectors.toSet());

        return Role.createEmbedded(role.keyspace(), role.getId(), role.getLabel(),
                sup, subs, role.isImplicit(), relationshipTypes, playedByTypes);
    }

    private static Rule buildRule(ai.grakn.concept.Rule rule, SchemaConcept sup, Set<SchemaConcept> subs){
        return Rule.createEmbedded(rule.keyspace(), rule.getId(), rule.getLabel(),
                sup, subs, rule.isImplicit(), rule.getWhen().toString(), rule.getThen().toString());
    }

    private static AttributeType buildAttributeType(ai.grakn.concept.AttributeType attributeType, SchemaConcept sup,
                                                    Set<SchemaConcept> subs, Set<Role> plays, Set<AttributeType> attributes,
                                                    Set<AttributeType> keys){
        return AttributeType.createEmbedded(attributeType.keyspace(), attributeType.getId(), attributeType.getLabel(),
                sup, subs, attributeType.isImplicit(), attributeType.isAbstract(), plays, attributes, keys);
    }

    private static EntityType buildEntityType(ai.grakn.concept.EntityType entityType, SchemaConcept sup,
                                                    Set<SchemaConcept> subs, Set<Role> plays, Set<AttributeType> attributes,
                                                    Set<AttributeType> keys){
        return EntityType.createEmbedded(entityType.keyspace(), entityType.getId(), entityType.getLabel(),
                sup, subs, entityType.isImplicit(), entityType.isAbstract(), plays, attributes, keys);
    }

    private static RelationshipType buildRelationshipType(ai.grakn.concept.RelationshipType relationshipType, SchemaConcept sup,
                                              Set<SchemaConcept> subs, Set<Role> plays, Set<AttributeType> attributes,
                                              Set<AttributeType> keys){
        Set<Role> relates = relationshipType.relates().
                map(ConceptBuilder::<Role>buildLinkOnlySchemaConcept).
                collect(Collectors.toSet());

        return RelationshipType.createEmbedded(relationshipType.keyspace(), relationshipType.getId(), relationshipType.getLabel(),
                sup, subs, relationshipType.isImplicit(), relationshipType.isAbstract(), plays, attributes, keys, relates);
    }

    /**
     * Builds  a {@link SchemaConcept} response which can only serialise into a link representation.
     */
    private static <X extends SchemaConcept> X buildLinkOnlySchemaConcept(ai.grakn.concept.SchemaConcept schemaConcept){
        if(schemaConcept.isRole()){
            return (X) Role.createLinkOnly(schemaConcept.keyspace(), schemaConcept.getId(), schemaConcept.getLabel());
        } else if (schemaConcept.isRule()){
            return (X) Rule.createLinkOnly(schemaConcept.keyspace(), schemaConcept.getId(), schemaConcept.getLabel());
        } else {
            return (X) buildLinkOnlyType(schemaConcept.asType());
        }
    }

    /**
     * Builds  a {@link Type} response which can only serialise into a link representation.
     */
    private static <X extends Type> X buildLinkOnlyType(ai.grakn.concept.Type type){
        if (type.isAttributeType()){
            return (X) AttributeType.createLinkOnly(type.keyspace(), type.getId(), type.getLabel());
        } else if (type.isEntityType()){
            return (X) EntityType.createLinkOnly(type.keyspace(), type.getId(), type.getLabel());
        } else if (type.isRelationshipType()){
            return (X) RelationshipType.createLinkOnly(type.keyspace(), type.getId(), type.getLabel());
        } else {
            throw GraknBackendException.convertingUnknownConcept(type);
        }
    }
}
