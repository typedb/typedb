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

import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.MatchQueryDefault;

import java.util.Map;
import java.util.Set;

/**
 * Abstract MatchQueryDefault implementation. Extends the abstract MatchQuery implementation, but provides extra behaviour
 * to support the MatchQueryDefault interface.
 */
abstract class MatchQueryDefaultAbstract
        extends MatchQueryAbstract<Map<String, Concept>, Map<String, Concept>> implements MatchQueryDefault.Admin {

    final MatchQueryDefault.Admin inner;

    MatchQueryDefaultAbstract(MatchQueryDefault.Admin inner) {
        super(inner);
        this.inner = inner;
    }

    @Override
    public Set<String> getSelectedNames() {
        return inner.getSelectedNames();
    }
}
