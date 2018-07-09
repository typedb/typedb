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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.client.concept;

import ai.grakn.client.Grakn;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import com.google.auto.value.AutoValue;

/**
 * Client implementation of {@link ai.grakn.concept.Entity}
 */
@AutoValue
public abstract class RemoteEntity extends RemoteThing<Entity, EntityType> implements Entity {

    static RemoteEntity construct(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteEntity(tx, id);
    }

    @Override
    final EntityType asCurrentType(Concept concept) {
        return concept.asEntityType();
    }

    @Override
    final Entity asCurrentBaseType(Concept other) {
        return other.asEntity();
    }
}
