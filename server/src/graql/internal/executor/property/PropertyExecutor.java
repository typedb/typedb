/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.internal.executor.property;

import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Set;

/**
 * A class describing an operation to perform using a VarProperty.
 * The behaviour is executed via a WriteExecutor using #execute. The class also
 * report its #requiredVars before it can run and its #producedVars(), that will be available to
 * other PropertyExecutors after it has run.
 * For example:
 * SubProperty property = SubProperty.of(y);
 * PropertyExecutor executor = property.define(x);
 * executor.requiredVars(); // returns `{y}`
 * executor.producedVars(); // returns `{x}`
 * // apply the `sub` property between `x` and `y`
 * // because it requires `y`, it will call `writeExecutor.get(y)`
 * // because it produces `x`, it will call `writeExecutor.builder(x)`
 * executor.execute(writeExecutor);
 */
public interface PropertyExecutor {

    interface Definable {

        Set<Writer> defineExecutors();

        Set<Writer> undefineExecutors();
    }

    interface Insertable {

        Set<Writer> insertExecutors();
    }

    interface Writer {

        Variable var();

        VarProperty property();

        /**
         * Get all Variables whose Concept must exist for the subject Variable to be applied.
         * For example, for IsaProperty the type must already be present before an instance can be created.
         * When calling #execute, the method can expect any Variable returned here to be available by calling
         * WriteExecutor#get.
         */
        Set<Variable> requiredVars();

        /**
         * Get all Variables whose Concept can only be created after this property is applied.
         * When calling #execute, the method must help build a Concept for every Variable returned
         * from this method, using WriteExecutor#builder.
         */
        Set<Variable> producedVars();

        /**
         * Apply the given property, if possible.
         *
         * @param executor a class providing a map of concepts that are accessible and methods to build new concepts.
         *                 This method can expect any key to be here that is returned from
         *                 #requiredVars(). The method may also build a concept provided that key is returned
         *                 from #producedVars().
         */
        void execute(WriteExecutor executor);
    }
}
