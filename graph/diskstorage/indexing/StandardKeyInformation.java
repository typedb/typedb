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

package grakn.core.graph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.Parameter;


public class StandardKeyInformation implements KeyInformation {

    private final Class<?> dataType;
    private final Parameter[] parameters;
    private final Cardinality cardinality;


    public StandardKeyInformation(Class<?> dataType, Cardinality cardinality, Parameter... parameters) {
        Preconditions.checkNotNull(dataType);
        Preconditions.checkNotNull(parameters);
        this.dataType = dataType;
        this.parameters = parameters;
        this.cardinality = cardinality;
    }

    public StandardKeyInformation(PropertyKey key, Parameter... parameters) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(parameters);
        this.dataType = key.dataType();
        this.parameters = parameters;
        this.cardinality = key.cardinality();
    }


    @Override
    public Class<?> getDataType() {
        return dataType;
    }

    public boolean hasParameters() {
        return parameters.length>0;
    }

    @Override
    public Parameter[] getParameters() {
        return parameters;
    }

    @Override
    public Cardinality getCardinality() {
        return cardinality;
    }

}
