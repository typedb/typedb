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

package ai.grakn.graql.admin;

import javax.annotation.CheckReturnValue;
import java.util.Set;

/**
 * A class representing a disjunction (or) of patterns. Any inner pattern must match in a query
 *
 * @param <T> the type of patterns in this disjunction
 *
 * @author Felix Chapman
 */
public interface Disjunction<T extends PatternAdmin> extends PatternAdmin {
    /**
     * @return the patterns within this disjunction
     */
    @CheckReturnValue
    Set<T> getPatterns();
}
