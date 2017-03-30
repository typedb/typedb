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

package ai.grakn;

/**
 * <p>
 *     A Class Describing the behaviour of a transaction
 * </p>
 *
 * <p>
 *     This class is used to describe how a transaction on {@link GraknGraph} should behave.
 *     When producing a graph using a {@link GraknSession} one of the following enums must be provided:
 *         READ - A read only transaction. If you attempt to mutate the graph with such a transaction an exception will be thrown.
 *         WRITE - A transaction which allows you to mutate the graph.
 *         BATCH - A transaction which allows mutations to be performed more quickly but disables some consitency checks.
 * </p>
 *
  @author fppt
 */
public enum GraknTxType {
    READ,  //Read only transaction where mutations to the graph are prohibited
    WRITE, //Write transaction where the graph can be mutated
    BATCH //Batch transaction which enables faster writes by switching off some consitency checks
}
