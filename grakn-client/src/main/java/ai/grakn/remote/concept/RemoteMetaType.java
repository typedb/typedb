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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote.concept;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.remote.RemoteGraknTx;
import com.google.auto.value.AutoValue;

/**
 * @author Felix Chapman
 */
@AutoValue
abstract class RemoteMetaType extends RemoteType<Type, Thing> {

    public static RemoteMetaType create(RemoteGraknTx tx, ConceptId id) {
        return new AutoValue_RemoteMetaType(tx, id);
    }

    @Override
    final Type asSelf(Concept concept) {
        return concept.asType();
    }

    @Override
    boolean isSelf(Concept concept) {
        return concept.isType();
    }

    @Override
    protected final Thing asInstance(Concept concept) {
        return concept.asThing();
    }
}
