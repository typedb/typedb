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
import ai.grakn.rpc.proto.ConceptProto;
import com.google.auto.value.AutoValue;

/**
 * Client implementation of a MetaType, a special type of {@link ai.grakn.concept.Type}
 *
 * TODO: This class is not defined in which is not defined in Core API, and at server side implementation.
 * TODO: we should remove this class, or implement properly on server side.
 */
@AutoValue
public abstract class RemoteEntityType extends RemoteType<EntityType, Entity> implements EntityType {

    static RemoteEntityType construct(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteEntityType(tx, id);
    }

    @Override
    public final Entity create() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setEntityTypeCreateReq(ConceptProto.EntityType.Create.Req.getDefaultInstance()).build();

        Concept concept = ConceptReader.concept(runMethod(method).getEntityTypeCreateRes().getEntity(), tx());
        return asInstance(concept);
    }

    @Override
    final EntityType asCurrentBaseType(Concept other) {
        return other.asEntityType();
    }

    @Override
    final boolean equalsCurrentBaseType(Concept other) {
        return other.isEntityType();
    }

    @Override
    protected final Entity asInstance(Concept concept) {
        return concept.asEntity();
    }
}
