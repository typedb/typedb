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

package ai.grakn.graql.shell;

/*-
 * #%L
 * grakn-graql-shell
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

import ai.grakn.concept.AttributeType;
import ai.grakn.graql.GraqlConverter;
import ai.grakn.graql.internal.printer.Printers;

import java.util.Set;

/**
 * Valid output formats for the Graql shell
 *
 * @author Felix Chapman
 */
public enum OutputFormat {
    Json {
        @Override
        GraqlConverter<?, String> getConverter(Set<AttributeType<?>> displayAttributes) {
            return Printers.json();
        }
    },

    Graql {
        @Override
        GraqlConverter<?, String> getConverter(Set<AttributeType<?>> displayAttributes) {
            AttributeType<?>[] array = displayAttributes.toArray(new AttributeType[displayAttributes.size()]);
            return Printers.graql(true, array);
        }
    };

    abstract GraqlConverter<?, String> getConverter(Set<AttributeType<?>> displayAttributes);

    static final OutputFormat DEFAULT = Graql;

    public static OutputFormat get(String name) {
        switch (name) {
            case "json":
                return Json;
            case "graql":
            default:
                return Graql;
        }
    }
}
