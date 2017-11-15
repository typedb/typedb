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
import com.google.auto.value.AutoValue;

import java.util.Set;

/**
 * <p>
 *     Wrapper class for {@link ai.grakn.concept.Entity}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class Entity extends Thing {

    public static Entity createEmbedded(
            ai.grakn.Keyspace keyspace,
            ConceptId conceptId,
            Set<Attribute> attributesLinked,
            Set<Attribute> keysLinked,
            Set<Relationship> relationshipsLinked){

        return new AutoValue_Entity(keyspace, conceptId, attributesLinked, keysLinked, relationshipsLinked);
    }

    public static Entity createLinkOnly(ai.grakn.Keyspace keyspace, ConceptId conceptId){
        return new AutoValue_Entity(keyspace, conceptId, null, null, null);
    }
}
