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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.controller.response;

import ai.grakn.engine.Jacksonisable;
import ai.grakn.kb.internal.concept.SchemaConceptImpl;
import ai.grakn.util.REST;
import ai.grakn.util.REST.WebPath;
import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;

import static java.util.stream.Collectors.joining;

/**
 * <p>
 *     A helper class used to represent a link between different wrapper {@link Concept}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class Link implements Jacksonisable{

    @JsonValue
    public abstract String id();

    @JsonCreator
    public static Link create(String id){
        return new AutoValue_Link(id);
    }

    public static Link create(Link link, Map<String, Object> params){
        String id = link.id() + "?" + params.entrySet().stream().map(Link::paramString).collect(joining("&"));
        return new AutoValue_Link(id);
    }

    private static String paramString(Map.Entry<String, Object> e) {
        return e.getKey() + "=" + e.getValue();
    }

    public static Link create(ai.grakn.concept.Thing thing){
        String id = REST.resolveTemplate(
                WebPath.CONCEPT_LINK,
                thing.keyspace().getValue(),
                Schema.BaseType.CONCEPT.name().toLowerCase(Locale.getDefault()),
                thing.getId().getValue());
        return create(id);
    }

    public static Link create(ai.grakn.concept.SchemaConcept schemaConcept){
        String type;

        if(schemaConcept.isType()){
            type = Schema.BaseType.TYPE.name().toLowerCase(Locale.getDefault());
        } else {
            type = SchemaConceptImpl.from(schemaConcept).baseType().name().toLowerCase(Locale.getDefault());
        }

        String id = REST.resolveTemplate(
                WebPath.CONCEPT_LINK,
                schemaConcept.keyspace().getValue(),
                type,
                schemaConcept.getLabel().getValue());
        return create(id);
    }

    /**
     * Creates a link to fetch all the {@link EmbeddedAttribute}s of a {@link Thing}
     */
    public static Link createAttributesLink(ai.grakn.concept.Thing thing){
        return create(REST.resolveTemplate(WebPath.CONCEPT_ATTRIBUTES, thing.keyspace().getValue(), thing.getId().getValue()));
    }

    /**
     * Creates a link to fetch all the {@link EmbeddedAttribute}s of a {@link Thing}
     */
    public static Link createKeysLink(ai.grakn.concept.Thing thing){
        return create(REST.resolveTemplate(WebPath.CONCEPT_KEYS, thing.keyspace().getValue(), thing.getId().getValue()));
    }

    /**
     * Creates a link to fetch all the {@link Relationship)s of a {@link Thing}
     */
    public static Link createRelationshipsLink(ai.grakn.concept.Thing thing){
        return create(REST.resolveTemplate(WebPath.CONCEPT_RELATIONSHIPS, thing.keyspace().getValue(), thing.getId().getValue()));
    }

    /**
     * Creates a link to fetch the instances of a {@link Type}
     */
    public static Link createInstanceLink(ai.grakn.concept.Type type){
        String id = REST.resolveTemplate(
                WebPath.TYPE_INSTANCES,
                type.keyspace().getValue(),
                type.getLabel().getValue());
        return create(id);
    }

    /**
     * Creates a link to get all the paged instances of a {@link Type}
     */
    public static Link createInstanceLink(ai.grakn.concept.Type type, int offset, int limit){
        ImmutableMap<String, Object> params = ImmutableMap.of(
                REST.Request.OFFSET_PARAMETER, offset, REST.Request.LIMIT_PARAMETER, limit
        );

        return create(createInstanceLink(type), params);
    }

    /**
     * Creates a link to get all the subs of a {@link SchemaConcept}
     */
    public static Link createSubsLink(ai.grakn.concept.SchemaConcept schemaConcept){
        String keyspace = schemaConcept.keyspace().getValue();
        String label = schemaConcept.getLabel().getValue();

        if(schemaConcept.isType()) {
            return create(REST.resolveTemplate(WebPath.TYPE_SUBS, keyspace, label));
        } else if (schemaConcept.isRole()){
            return create(REST.resolveTemplate(WebPath.ROLE_SUBS, keyspace, label));
        } else {
            return create(REST.resolveTemplate(WebPath.RULE_SUBS, keyspace, label));
        }
    }

    /**
     * Creates a link to get all the plays of a {@link Type}
     */
    public static Link createPlaysLink(ai.grakn.concept.Type type){
        return create(REST.resolveTemplate(WebPath.TYPE_PLAYS, type.keyspace().getValue(), type.getLabel().getValue()));
    }

    /**
     * Creates a link to get all the attributes of a {@link Type}
     */
    public static Link createAttributesLink(ai.grakn.concept.Type type){
        return create(REST.resolveTemplate(WebPath.TYPE_ATTRIBUTES, type.keyspace().getValue(), type.getLabel().getValue()));
    }

    /**
     * Creates a link to get all the keys of a {@link Type}
     */
    public static Link createKeysLink(ai.grakn.concept.Type type){
        return create(REST.resolveTemplate(WebPath.TYPE_KEYS, type.keyspace().getValue(), type.getLabel().getValue()));
    }

    /**
     * Creates a link to get all the instances of a {@link Type}
     */
    public static Link createInstancesLink(ai.grakn.concept.Type type){
        return create(REST.resolveTemplate(WebPath.TYPE_INSTANCES, type.keyspace().getValue(), type.getLabel().getValue()));
    }

}
