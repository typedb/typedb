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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;


public class SimpleAddedRelations extends ArrayList<InternalRelation> implements AddedRelationsContainer {

    private static final int INITIAL_ADDED_SIZE = 10;

    public SimpleAddedRelations() {
        super(INITIAL_ADDED_SIZE);
    }

    @Override
    public boolean add(InternalRelation relation) {
        return super.add(relation);
    }

    @Override
    public boolean remove(InternalRelation relation) {
        return super.remove(relation);
    }

    @Override
    public List<InternalRelation> getView(Predicate<InternalRelation> filter) {
        final List<InternalRelation> result = new ArrayList<>();
        for (InternalRelation r : this) {
            if (filter.test(r)) result.add(r);
        }
        return result;
    }

    @Override
    public Collection<InternalRelation> getAll() {
        return this;
    }
}
