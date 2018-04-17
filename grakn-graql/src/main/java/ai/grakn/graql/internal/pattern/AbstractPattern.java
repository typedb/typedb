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

package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.admin.PatternAdmin;

/**
 * The abstract implementation of {@link PatternAdmin}.
 *
 * All implementations of {@link PatternAdmin} should extend this class to inherit certain default behaviours.
 *
 * @author Felix Chapman
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
