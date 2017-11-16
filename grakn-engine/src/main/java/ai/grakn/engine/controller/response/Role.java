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
import ai.grakn.concept.Label;
import ai.grakn.util.Schema;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Role}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class Role extends SchemaConcept{

    @Nullable
    public abstract Set<RelationshipType> relationshipTypes();

    @Nullable
    public abstract Set<Type> roleplayerTypes();

    @JsonProperty
    public Set<String> relationships(){
        return extractIds(relationshipTypes());
    }

    @JsonProperty
    public Set<String> roleplayers(){
        return extractIds(roleplayerTypes());
    }

    @Override
    public Schema.BaseType baseType(){
        return Schema.BaseType.ROLE;
    }

    public static Role createEmbedded(
        ai.grakn.Keyspace keyspace,
        ConceptId conceptId,
        Label label,
        SchemaConcept superConcept,
        Set<SchemaConcept> subConcepts,
        Boolean isImplicit,
        Set<RelationshipType> relationshipTypes,
        Set<Type> roleplayerTypes){

        return new AutoValue_Role(keyspace, conceptId, superConcept, subConcepts, label, isImplicit, relationshipTypes, roleplayerTypes);
    }

    public static Role createLinkOnly(ai.grakn.Keyspace keyspace, ConceptId conceptId, Label label){
        return new AutoValue_Role(keyspace, conceptId, null, null, label, null, null, null);
    }
}
