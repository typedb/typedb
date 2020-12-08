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
 *
 */

package grakn.core.logic.tool;

import grakn.core.common.cache.CommonCache;
import grakn.core.common.parameters.Label;
import grakn.core.pattern.Conjunction;
import graql.lang.pattern.variable.Reference;

import java.util.Map;
import java.util.Set;

public class TypeHinterCache extends CommonCache<Conjunction, Map<Reference, Set<Label>>> {

    public TypeHinterCache() {
        super();
    }

    public TypeHinterCache(int size, int timeOutMinutes) {
        super(size, timeOutMinutes);
    }
}
