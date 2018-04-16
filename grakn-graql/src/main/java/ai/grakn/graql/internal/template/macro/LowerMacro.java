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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.template.macro;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.macro.Macro;

import java.util.List;
import java.util.Locale;

/**
 * <p>
 * Convert the given value into an lower case string. Only accepts one argument.
 *
 * Usage:
 *      {@literal @}lower(<value>)
 * </p>
 *
 * @author alexandraorth
 */
public class LowerMacro implements Macro<String> {

    private static final int numberArguments = 1;

    @Override
    public String apply(List<Object> values) {
        if(values.size() != numberArguments){
            throw GraqlQueryException.wrongNumberOfMacroArguments(this, values);
        }

        return values.get(0).toString().toLowerCase(Locale.getDefault());
    }

    @Override
    public String name() {
        return "lower";
    }
}
