/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.transaction.addedrelations;

import grakn.core.graph.graphdb.internal.InternalRelation;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public interface AddedRelationsContainer {

    boolean add(InternalRelation relation);

    boolean remove(InternalRelation relation);

    List<InternalRelation> getView(Predicate<InternalRelation> filter);

    boolean isEmpty();

    /**
     * This method returns all relations in this container. It may only be invoked at the end
     * of the transaction after there are no additional changes. Otherwise the behavior is non deterministic.
     */
    Collection<InternalRelation> getAll();


    AddedRelationsContainer EMPTY = new AddedRelationsContainer() {
        @Override
        public boolean add(InternalRelation relation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(InternalRelation relation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<InternalRelation> getView(Predicate<InternalRelation> filter) {
            return Collections.emptyList();
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public Collection<InternalRelation> getAll() {
            return Collections.emptyList();
        }
    };

}
