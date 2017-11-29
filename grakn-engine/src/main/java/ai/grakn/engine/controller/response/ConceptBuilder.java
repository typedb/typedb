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
import ai.grakn.util.Schema;

import java.util.HashSet;
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
    public static <X extends Concept> X build(ai.grakn.concept.Concept concept){
        Concept response;
        if(concept.isSchemaConcept()){
            response = buildSchemaConcept(concept.asSchemaConcept());
        } else if (concept.isThing()) {
            response = buildThing(concept.asThing());
        } else {
            throw GraknBackendException.convertingUnknownConcept(concept);
        }

        return (X) response;
    }

    /**
     * Gets all the instances of a specific {@link ai.grakn.concept.Type} and wraps them in a {@link Things}
     * response object
     *
     * @param type The {@link ai.grakn.concept.Type} to extract the {@link ai.grakn.concept.Thing}s from
     * @return The wrapper of the {@link ai.grakn.concept.Thing}s
     */
    public static Things buildThings(ai.grakn.concept.Type type){
        return buildThings(type, 0, 100);
    }

    public static Things buildThingsWithOffset(ai.grakn.concept.Type type, int offset){
        return buildThings(type, offset, 100);
    }

    public static Things buildThingsWithLimit(ai.grakn.concept.Type type, int limit){
        return buildThings(type, 0, limit);
    }

    public static Things buildThings(ai.grakn.concept.Type type, int offset, int limit){
        Link selfLink = Link.createInstanceLink(type);
        Link next = Link.createInstanceLink(type, offset + limit, limit);

        int previousIndex = offset - limit;
        if(previousIndex < 0) previousIndex = 0;
        Link previous = Link.createInstanceLink(type, previousIndex, limit);

        //TODO: This does not actually scale. The DB is still read in this instance
        Set<Thing> things = type.instances().skip(offset).limit(limit).
                map(ConceptBuilder::buildThing).collect(Collectors.toSet());

        return Things.create(selfLink, things, next, previous);
    }

    //TODO: This will scale poorly with super nodes. Need to introduce some sort of paging maybe?
    private static Thing buildThing(ai.grakn.concept.Thing thing) {
        Link selfLink = Link.create(thing);
        Link type = Link.create(thing.type());
        Set<Link> attributes = thing.attributes().map(Link::create).collect(Collectors.toSet());
        Set<Link> keys = thing.keys().map(Link::create).collect(Collectors.toSet());

        Set<RolePlayer> relationships = new HashSet<>();
        thing.plays().forEach(role -> {
            Link roleWrapper = Link.create(role);
            thing.relationships(role).forEach(relationship -> {
                Link relationshipWrapper = Link.create(relationship);
                relationships.add(RolePlayer.create(roleWrapper, relationshipWrapper));
            });
        });

        if(thing.isAttribute()){
            return buildAttribute(thing.asAttribute(), selfLink, type, attributes, keys, relationships);
        } else if (thing.isRelationship()){
            return buildRelationship(thing.asRelationship(), selfLink, type, attributes, keys, relationships);
        } else if (thing.isEntity()){
            return buildEntity(thing.asEntity(), selfLink, type, attributes, keys, relationships);
        } else {
            throw GraknBackendException.convertingUnknownConcept(thing);
        }
    }

    private static SchemaConcept buildSchemaConcept(ai.grakn.concept.SchemaConcept schemaConcept){
        Link selfLink = Link.create(schemaConcept);

        Link sup = null;
        if(schemaConcept.sup() != null) sup = Link.create(schemaConcept.sup());

        Set<Link> subs = schemaConcept.subs().map(Link::create).collect(Collectors.toSet());

        if(Schema.MetaSchema.THING.getLabel().equals(schemaConcept.getLabel())) {
            return MetaConcept.create(schemaConcept.getId(), selfLink, schemaConcept.getLabel(),  sup, subs);
        } else if(schemaConcept.isRole()){
            return buildRole(schemaConcept.asRole(), selfLink, sup, subs);
        } else if(schemaConcept.isRule()){
            return buildRule(schemaConcept.asRule(), selfLink, sup, subs);
        } else {
            return buildType(schemaConcept.asType(), selfLink, sup, subs);
        }
    }

    private static Entity buildEntity(ai.grakn.concept.Entity entity, Link selfLink, Link type, Set<Link> attributes, Set<Link> keys, Set<RolePlayer> relationships){
        return Entity.create(entity.getId(), selfLink, type, attributes, keys, relationships);
    }

    private static Attribute buildAttribute(ai.grakn.concept.Attribute attribute, Link selfLink, Link type, Set<Link> attributes, Set<Link> keys, Set<RolePlayer> relationships){
        return Attribute.create(attribute.getId(), selfLink, type, attributes, keys, relationships, attribute.type().getDataType().getName(), attribute.getValue().toString());
    }

    private static Relationship buildRelationship(ai.grakn.concept.Relationship relationship, Link selfLink, Link type, Set<Link> attributes, Set<Link> keys, Set<RolePlayer> relationships){
        //Get all the role players and roles part of this relationship
        Set<RolePlayer> roleplayers = new HashSet<>();
        relationship.allRolePlayers().forEach((role, things) -> {
            Link roleLink = Link.create(role);
            things.forEach(thing -> roleplayers.add(RolePlayer.create(roleLink, Link.create(thing))));
        });
        return Relationship.create(relationship.getId(), selfLink, type, attributes, keys, relationships, roleplayers);
    }

    private static Type buildType(ai.grakn.concept.Type type, Link selfLink, Link sup, Set<Link> subs){
        Set<Link> roles = type.plays().
                map(Link::create).
                collect(Collectors.toSet());

        Set<Link> attributes = type.attributes().
                map(Link::create).
                collect(Collectors.toSet());

        Set<Link> keys = type.keys().
                map(Link::create).
                collect(Collectors.toSet());

        if(type.isAttributeType()){
            return buildAttributeType(type.asAttributeType(), selfLink, sup, subs, roles, attributes, keys);
        } else if (type.isEntityType()){
            return buildEntityType(type.asEntityType(), selfLink, sup, subs, roles, attributes, keys);
        } else if (type.isRelationshipType()){
            return buildRelationshipType(type.asRelationshipType(), selfLink, sup, subs, roles, attributes, keys);
        } else {
            throw GraknBackendException.convertingUnknownConcept(type);
        }
    }

    private static Role buildRole(ai.grakn.concept.Role role, Link selfLink, Link sup, Set<Link> subs){
        Set<Link> relationships = role.relationshipTypes().map(Link::create).collect(Collectors.toSet());
        Set<Link> roleplayers = role.playedByTypes().map(Link::create).collect(Collectors.toSet());
        return Role.create(role.getId(), selfLink, role.getLabel(), role.isImplicit(), sup, subs,
                relationships, roleplayers);
    }

    private static Rule buildRule(ai.grakn.concept.Rule rule, Link selfLink, Link sup, Set<Link> subs){
        String when = null;
        if(rule.getWhen() != null) when = rule.getWhen().toString();

        String then = null;
        if(rule.getThen() != null) then = rule.getThen().toString();

        return Rule.create(rule.getId(), selfLink, rule.getLabel(), rule.isImplicit(), sup, subs, when, then);
    }

    private static AttributeType buildAttributeType(ai.grakn.concept.AttributeType attributeType, Link selfLink, Link sup,
                                                    Set<Link> subs, Set<Link> plays, Set<Link> attributes,
                                                    Set<Link> keys){
        String dataType = null;
        if(attributeType.getDataType() != null) dataType = attributeType.getDataType().getName();

        return AttributeType.create(attributeType.getId(), selfLink, attributeType.getLabel(), attributeType.isImplicit(),
                sup, subs, attributeType.isAbstract(), plays, attributes, keys, dataType, attributeType.getRegex());
    }

    private static EntityType buildEntityType(ai.grakn.concept.EntityType entityType, Link selfLink, Link sup,
                                              Set<Link> subs, Set<Link> plays, Set<Link> attributes, Set<Link> keys){
        return EntityType.create(entityType.getId(), selfLink, entityType.getLabel(), entityType.isImplicit(),
                sup, subs, entityType.isAbstract(), plays, attributes, keys);
    }

    private static RelationshipType buildRelationshipType(ai.grakn.concept.RelationshipType relationshipType,
                                                          Link selfLink, Link sup, Set<Link> subs,
                                                          Set<Link> plays, Set<Link> attributes,
                                                          Set<Link> keys){
        Set<Link> relates = relationshipType.relates().
                map(Link::create).
                collect(Collectors.toSet());

        return RelationshipType.create(relationshipType.getId(), selfLink, relationshipType.getLabel(), relationshipType.isImplicit(),
                sup, subs, relationshipType.isAbstract(), plays, attributes, keys, relates);
    }
}
