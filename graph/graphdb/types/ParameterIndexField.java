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
 */

package grakn.core.graph.graphdb.types;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.core.schema.SchemaStatus;

public class ParameterIndexField extends IndexField {

    private final Parameter[] parameters;

    private ParameterIndexField(PropertyKey key, Parameter[] parameters) {
        super(key);
        this.parameters = Preconditions.checkNotNull(parameters);
    }

    public SchemaStatus getStatus() {
        SchemaStatus status = ParameterType.STATUS.findParameter(parameters, null);
        return Preconditions.checkNotNull(status, "Field [%s] did not have a status", this);
    }

    public Parameter[] getParameters() {
        return parameters;
    }

    public static ParameterIndexField of(PropertyKey key, Parameter... parameters) {
        return new ParameterIndexField(key, parameters);
    }


}
