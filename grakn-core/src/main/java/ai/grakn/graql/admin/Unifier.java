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

package ai.grakn.graql.admin;

import ai.grakn.graql.VarName;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface Unifier{

    VarName get(VarName key);
    VarName put(VarName key, VarName value);

    boolean isEmpty();

    Map<VarName, VarName> map();

    Set<VarName> keySet();
    Collection<VarName> values();
    Set<Map.Entry<VarName, VarName>> entrySet();

    boolean containsKey(VarName key);
    boolean containsValue(VarName value);

    Unifier merge(Unifier d);


    Unifier removeTrivialEntries();

    int size();
}
