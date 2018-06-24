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

package ai.grakn.remote.concept;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.remote.Grakn;
import com.google.auto.value.AutoValue;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteMetaType extends RemoteType<Type, Thing> {

    public static RemoteMetaType create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteMetaType(tx, id);
    }

    @Override
    final Type asCurrentBaseType(Concept other) {
        return other.asType();
    }

    @Override
    boolean equalsCurrentBaseType(Concept other) {
        return other.isType();
    }

    @Override
    protected final Thing asInstance(Concept concept) {
        return concept.asThing();
    }
}
