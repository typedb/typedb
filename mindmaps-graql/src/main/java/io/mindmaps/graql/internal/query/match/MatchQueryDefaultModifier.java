/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package io.mindmaps.graql.internal.query.match;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.admin.MatchQueryDefaultAdmin;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.internal.query.Conjunction;

import java.util.Optional;
import java.util.Set;

/**
 * Abstract MatchQueryDefault implementation. Extends the abstract MatchQuery implementation, but provides extra behaviour
 * to support the MatchQueryDefault interface.
 */
abstract class MatchQueryDefaultModifier extends AbstractMatchQueryDefault {

    final MatchQueryDefaultAdmin inner;

    MatchQueryDefaultModifier(MatchQueryDefaultAdmin inner) {
        this.inner = inner;
    }

    @Override
    public Set<Type> getTypes(MindmapsGraph transaction) {
        return inner.getTypes(transaction);
    }

    @Override
    public Set<Type> getTypes() {
        return inner.getTypes();
    }

    @Override
    public Conjunction<PatternAdmin> getPattern() {
        return inner.getPattern();
    }

    @Override
    public Optional<MindmapsGraph> getTransaction() {
        return inner.getTransaction();
    }

    @Override
    public Set<String> getSelectedNames() {
        return inner.getSelectedNames();
    }
}
