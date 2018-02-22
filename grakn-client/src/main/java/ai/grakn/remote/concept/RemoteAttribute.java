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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.remote.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.remote.RemoteGraknTx;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * @author Felix Chapman
 *
 * @param <D> The data type of this attribute
 */
@AutoValue
abstract class RemoteAttribute<D extends AttributeType.DataType<?>>
        extends RemoteThing<Attribute<D>, AttributeType<D>> implements Attribute<D> {

    public static <D extends AttributeType.DataType<?>> RemoteAttribute<D> create(RemoteGraknTx tx, ConceptId id) {
        return new AutoValue_RemoteAttribute<>(tx, id);
    }

    @Override
    public final D getValue() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final AttributeType.DataType<D> dataType() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Thing> ownerInstances() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Nullable
    @Override
    public final Thing owner() {
        throw new UnsupportedOperationException(); // TODO: implement
    }
}
