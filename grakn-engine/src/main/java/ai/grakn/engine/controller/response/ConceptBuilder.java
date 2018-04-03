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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.controller.response;

import ai.grakn.exception.GraknBackendException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.reasoner.utils.conversion.ConceptConverter;
import ai.grakn.util.Schema;

import java.util.HashSet;
import java.util.List;
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
    public static Things buildThings(ai.grakn.concept.Type type, int offset, int limit){
        Link selfLink = Link.createInstanceLink(type, offset, limit);

        Link previous = null;

        if (offset != 0) {
            int previousIndex = offset - limit;
            if (previousIndex < 0) previousIndex = 0;
            previous = Link.createInstanceLink(type, previousIndex, limit);
        }

        //TODO: This does not actually scale. The DB is still read in this instance
        List<Thing> things = type.instances().skip(offset).limit(limit + 1L).
                map(ConceptBuilder::buildThing).collect(Collectors.toList());

        // We get one extra instance and then remove it so we can sneakily check if there is a next page
        boolean hasNextPage = things.size() == limit + 1;
        Link next = hasNextPage ? Link.createInstanceLink(type, offset + limit, limit) : null;
        if (things.size() == limit + 1) things.remove(things.size() - 1);

        return Things.create(selfLink, things, next, previous);
    }

    //TODO: This will scale poorly with super nodes. Need to introduce some sort of paging maybe?
    private static Thing buildThing(ai.grakn.concept.Thing thing) {
        Link selfLink = Link.create(thing);
        EmbeddedSchemaConcept type = EmbeddedSchemaConcept.create(thing.type());
        Link attributes = Link.createAttributesLink(thing);
        Link keys = Link.createKeysLink(thing);
        Link relationships = Link.createRelationshipsLink(thing);

        String explanation = null;
        if(thing.isInferred()) explanation = Graql.match(ConceptConverter.toPattern(thing)).get().toString();

        if(thing.isAttribute()){
            return buildAttribute(thing.asAttribute(), selfLink, type, attributes, keys, relationships, explanation);
        } else if (thing.isRelationship()){
            return buildRelationship(thing.asRelationship(), selfLink, type, attributes, keys, relationships, explanation);
        } else if (thing.isEntity()){
            return buildEntity(thing.asEntity(), selfLink, type, attributes, keys, relationships, explanation);
        } else {
            throw GraknBackendException.convertingUnknownConcept(thing);
        }
    }

    private static SchemaConcept buildSchemaConcept(ai.grakn.concept.SchemaConcept schemaConcept){
        Link selfLink = Link.create(schemaConcept);

        EmbeddedSchemaConcept sup = null;
        if(schemaConcept.sup() != null) sup = EmbeddedSchemaConcept.create(schemaConcept.sup());

        Link subs = Link.createSubsLink(schemaConcept);

        if(schemaConcept.isRole()){
            return buildRole(schemaConcept.asRole(), selfLink, sup, subs);
        } else if(schemaConcept.isRule()){
            return buildRule(schemaConcept.asRule(), selfLink, sup, subs);
        } else {
            return buildType(schemaConcept.asType(), selfLink, sup, subs);
        }
    }

    private static Entity buildEntity(ai.grakn.concept.Entity entity, Link selfLink, EmbeddedSchemaConcept type, Link attributes, Link keys, Link relationships, String explanation){
        return Entity.create(entity.getId(), selfLink, type, attributes, keys, relationships, entity.isInferred(), explanation);
    }

    private static Attribute buildAttribute(ai.grakn.concept.Attribute attribute, Link selfLink, EmbeddedSchemaConcept type, Link attributes, Link keys, Link relationships, String explanation){
        return Attribute.create(attribute.getId(), selfLink, type, attributes, keys, relationships, attribute.isInferred(), explanation, attribute.type().getDataType().getName(), attribute.getValue().toString());
    }

    private static Relationship buildRelationship(ai.grakn.concept.Relationship relationship, Link selfLink, EmbeddedSchemaConcept type, Link attributes, Link keys, Link relationships, String explanation){
        //Get all the role players and roles part of this relationship
        Set<RolePlayer> roleplayers = new HashSet<>();
        relationship.allRolePlayers().forEach((role, things) -> {
            Link roleLink = Link.create(role);
            things.forEach(thing -> roleplayers.add(RolePlayer.create(roleLink, Link.create(thing))));
        });
        return Relationship.create(relationship.getId(), selfLink, type, attributes, keys, relationships, relationship.isInferred(), explanation, roleplayers);
    }

    private static Type buildType(ai.grakn.concept.Type type, Link selfLink, EmbeddedSchemaConcept sup, Link subs){
        Link plays = Link.createPlaysLink(type);
        Link attributes = Link.createAttributesLink(type);
        Link keys = Link.createKeysLink(type);
        Link instances = Link.createInstancesLink(type);

        if(Schema.MetaSchema.THING.getLabel().equals(type.getLabel())) {
            return MetaConcept.create(type.getId(), selfLink, type.getLabel(),  sup, subs, plays, attributes, keys, instances);
        } else if(type.isAttributeType()){
            return buildAttributeType(type.asAttributeType(), selfLink, sup, subs, plays, attributes, keys, instances);
        } else if (type.isEntityType()){
            return buildEntityType(type.asEntityType(), selfLink, sup, subs, plays, attributes, keys, instances);
        } else if (type.isRelationshipType()){
            return buildRelationshipType(type.asRelationshipType(), selfLink, sup, subs, plays, attributes, keys, instances);
        } else {
            throw GraknBackendException.convertingUnknownConcept(type);
        }
    }

    private static Role buildRole(ai.grakn.concept.Role role, Link selfLink, EmbeddedSchemaConcept sup, Link subs){
        Set<Link> relationships = role.relationshipTypes().map(Link::create).collect(Collectors.toSet());
        Set<Link> roleplayers = role.playedByTypes().map(Link::create).collect(Collectors.toSet());
        return Role.create(role.getId(), selfLink, role.getLabel(), role.isImplicit(), sup, subs,
                relationships, roleplayers);
    }

    private static Rule buildRule(ai.grakn.concept.Rule rule, Link selfLink, EmbeddedSchemaConcept sup, Link subs){
        String when = null;
        if(rule.getWhen() != null) when = rule.getWhen().toString();

        String then = null;
        if(rule.getThen() != null) then = rule.getThen().toString();

        return Rule.create(rule.getId(), selfLink, rule.getLabel(), rule.isImplicit(), sup, subs, when, then);
    }

    private static AttributeType buildAttributeType(ai.grakn.concept.AttributeType attributeType, Link selfLink, EmbeddedSchemaConcept sup,
                                                    Link subs, Link plays, Link attributes,
                                                    Link keys, Link instances){
        String dataType = null;
        if(attributeType.getDataType() != null) dataType = attributeType.getDataType().getName();

        return AttributeType.create(attributeType.getId(), selfLink, attributeType.getLabel(), attributeType.isImplicit(),
                sup, subs, attributeType.isAbstract(), plays, attributes, keys, instances, dataType, attributeType.getRegex());
    }

    private static EntityType buildEntityType(ai.grakn.concept.EntityType entityType, Link selfLink, EmbeddedSchemaConcept sup,
                                              Link subs, Link plays, Link attributes, Link keys, Link instances){
        return EntityType.create(entityType.getId(), selfLink, entityType.getLabel(), entityType.isImplicit(),
                sup, subs, entityType.isAbstract(), plays, attributes, keys, instances);
    }

    private static RelationshipType buildRelationshipType(ai.grakn.concept.RelationshipType relationshipType,
                                                          Link selfLink, EmbeddedSchemaConcept sup, Link subs,
                                                          Link plays, Link attributes,
                                                          Link keys, Link instances){
        Set<Link> relates = relationshipType.relates().
                map(Link::create).
                collect(Collectors.toSet());

        return RelationshipType.create(relationshipType.getId(), selfLink, relationshipType.getLabel(), relationshipType.isImplicit(),
                sup, subs, relationshipType.isAbstract(), plays, attributes, keys, instances, relates);
    }
}
