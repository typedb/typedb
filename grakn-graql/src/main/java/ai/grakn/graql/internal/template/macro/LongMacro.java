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
 */

package ai.grakn.graql.internal.template.macro;

import ai.grakn.graql.macro.Macro;

import java.util.List;

/**
 * Macro that will convert the given value to a long
 */
public class LongMacro implements Macro<Long> {

    private static final int numberArguments = 1;

    @Override
    public Long apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw new IllegalArgumentException("Wrong number of arguments [" + values.size() + "] to macro " + name());
        }

        return Long.parseLong(values.get(0).toString());
    }

    @Override
    public String name() {
        return "long";
    }
}
