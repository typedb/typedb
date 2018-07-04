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
import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.rpc.proto.MethodProto;
import ai.grakn.rpc.proto.SessionProto;
import com.google.auto.value.AutoValue;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteEntityType extends RemoteType<EntityType, Entity> implements EntityType {

    public static RemoteEntityType create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteEntityType(tx, id);
    }

    @Override
    public final Entity addEntity() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setAddEntity(MethodProto.AddEntity.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        Concept concept = ConceptBuilder.concept(response.getConceptMethod().getResponse().getAddEntity().getConcept(), tx());
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
