/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.client.concept;

import ai.grakn.client.Grakn;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import com.google.auto.value.AutoValue;

/**
 * Client implementation of {@link ai.grakn.concept.Type}
 */
@AutoValue
public abstract class RemoteMetaType extends RemoteType<Type, Thing> {

    static RemoteMetaType construct(Grakn.Transaction tx, ConceptId id) {
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
