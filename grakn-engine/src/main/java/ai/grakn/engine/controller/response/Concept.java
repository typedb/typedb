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

import ai.grakn.concept.ConceptId;
import ai.grakn.util.REST;
import ai.grakn.util.REST.WebPath;
import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Concept}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public abstract class Concept {

    public abstract ai.grakn.Keyspace keyspace();

    public abstract ConceptId conceptId();

    public Schema.BaseType baseType(){
        return Schema.BaseType.CONCEPT;
    }

    public String uniqueId(){
        return conceptId().getValue();
    }

    @JsonProperty("@id")
    public String id(){
        return REST.resolveTemplate(WebPath.CONCEPT_ID,
                keyspace().getValue(),
                baseType().getClassType().getName().toLowerCase(),
                uniqueId());
    }

    public static Set<String> extractIds(Set<? extends Concept> concepts){
        return concepts.stream().map(Concept::id).collect(Collectors.toSet());
    }
}
