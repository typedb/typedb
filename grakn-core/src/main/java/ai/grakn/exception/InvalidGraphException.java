/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.exception;

import ai.grakn.GraknGraph;

/**
 * <p>
 *     Broken Graph Exception
 * </p>
 *
 * <p>
 *     This exception is thrown on {@link GraknGraph#commit()} when the graph does not comply with the grakn
 *     validation rules. For a complete list of these rules please refer to the documentation
 * </p>
 *
 * @author fppt
 */
public class InvalidGraphException extends GraknException{
}
