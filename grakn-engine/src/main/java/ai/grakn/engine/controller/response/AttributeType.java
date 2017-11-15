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
import com.google.auto.value.AutoValue;

import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.AttributeType}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class AttributeType extends Type{

    public static AttributeType createEmbedded(
            ai.grakn.Keyspace keyspace,
            ConceptId conceptId,
            SchemaConcept superConcept,
            Set<SchemaConcept> subConcepts,
            Label label,
            Boolean isImplicit,
            Boolean isAbstract,
            Set<Role> rolesPlayed,
            Set<AttributeType> attributesLinked,
            Set<AttributeType> keysLinked){

        return new AutoValue_AttributeType(keyspace, conceptId, superConcept, subConcepts, label, isImplicit, isAbstract, rolesPlayed, attributesLinked, keysLinked);
    }

    public static AttributeType createLinkOnly(ai.grakn.Keyspace keyspace, ConceptId conceptId, Label label){
        return new AutoValue_AttributeType(keyspace, conceptId, null, null, label, null, null, null, null, null);
    }
}
