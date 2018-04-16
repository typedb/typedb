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

package ai.grakn.exception;

/*-
 * #%L
 * grakn-core
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

import org.apache.tinkerpop.gremlin.structure.Vertex;

import static ai.grakn.util.ErrorMessage.LOCKING_EXCEPTION;

/**
 * <p>
 *     Mutation Exception
 * </p>
 *
 * <p>
 *     This exception occurs when we are temporarily unable to write to the graph.
 *     This is typically caused by the persistence layer being overloaded.
 *     When this occurs the transaction should be retried
 * </p>
 *
 * @author fppt
 */
public class TemporaryWriteException extends GraknBackendException{
    private TemporaryWriteException(String error, Exception e) {
        super(error, e);
    }

    /**
     * Thrown when the persistence layer is locked temporarily.
     * Retrying the transaction is reccomended.
     */
    public static TemporaryWriteException temporaryLock(Exception e){
        return new TemporaryWriteException(LOCKING_EXCEPTION.getMessage(), e);
    }

    /**
     * Thrown when multiple transactions overlap in using an index. This results in incomplete vertices being shared
     * between transactions.
     */
    public static TemporaryWriteException indexOverlap(Vertex vertex, Exception e){
        return new TemporaryWriteException(String.format("Index overlap has led to the accidental sharing of a partially complete vertex {%s}", vertex), e);
    }
}
