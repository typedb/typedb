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

package grakn.core.graql.internal.pattern;

import grakn.core.graql.Graql;
import grakn.core.graql.Pattern;
import grakn.core.graql.admin.PatternAdmin;

/**
 * The abstract implementation of {@link PatternAdmin}.
 *
 * All implementations of {@link PatternAdmin} should extend this class to inherit certain default behaviours.
 *
 */
public abstract class AbstractPattern implements PatternAdmin {

    @Override
    public final Pattern and(Pattern pattern) {
        return Graql.and(this, pattern);
    }

    @Override
    public final Pattern or(Pattern pattern) {
        return Graql.or(this, pattern);
    }
}
