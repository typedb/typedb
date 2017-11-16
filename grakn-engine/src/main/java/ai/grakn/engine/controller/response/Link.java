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

import ai.grakn.engine.Jacksonisable;
import ai.grakn.util.REST;
import ai.grakn.util.REST.WebPath;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import java.util.Locale;

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

    public static Link create(ai.grakn.concept.Thing thing){
        String id = REST.resolveTemplate(
                WebPath.CONCEPT_ID,
                thing.keyspace().getValue(),
                Schema.BaseType.CONCEPT.name().toLowerCase(Locale.getDefault()),
                thing.getId().getValue());
        return new AutoValue_Link(id);
    }

    public static Link create(ai.grakn.concept.Type type){
        String id = REST.resolveTemplate(
                WebPath.CONCEPT_ID,
                type.keyspace().getValue(),
                Schema.BaseType.TYPE.name().toLowerCase(Locale.getDefault()),
                type.getLabel().getValue());
        return new AutoValue_Link(id);
    }

    public static Link create(ai.grakn.concept.Rule rule){
        String id = REST.resolveTemplate(
                WebPath.CONCEPT_ID,
                rule.keyspace().getValue(),
                Schema.BaseType.TYPE.name().toLowerCase(Locale.getDefault()),
                rule.getLabel().getValue());
        return new AutoValue_Link(id);
    }

    public static Link create(ai.grakn.concept.Role role){
        String id = REST.resolveTemplate(
                WebPath.CONCEPT_ID,
                role.keyspace().getValue(),
                Schema.BaseType.TYPE.name().toLowerCase(Locale.getDefault()),
                role.getLabel().getValue());
        return new AutoValue_Link(id);
    }
}
