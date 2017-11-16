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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Relationship}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class Relationship extends Thing {

    @Nullable
    @JsonProperty
    public abstract Set<RolePlayer> roleplayers();

    public static Relationship createEmbedded(
            ai.grakn.Keyspace keyspace,
            ConceptId conceptId,
            Set<Attribute> attributesLinked,
            Set<Attribute> keysLinked,
            Set<Relationship> relationshipsLinked,
            Set<RolePlayer> roleplayers){

        return new AutoValue_Relationship(keyspace, conceptId, attributesLinked, keysLinked, relationshipsLinked, roleplayers);
    }

    public static Relationship createLinkOnly(ai.grakn.Keyspace keyspace, ConceptId conceptId){
        return new AutoValue_Relationship(keyspace, conceptId, null, null, null, null);
    }
}
