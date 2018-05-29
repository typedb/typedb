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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn;

/**
 * An enum that determines the type of Grakn Transaction.
 *
 * This class is used to describe how a transaction on {@link GraknTx} should behave.
 * When producing a graph using a {@link GraknSession} one of the following enums must be provided:
 * READ - A read only transaction. If you attempt to mutate the graph with such a transaction an exception will be thrown.
 * WRITE - A transaction which allows you to mutate the graph.
 * BATCH - A transaction which allows mutations to be performed more quickly but disables some consitency checks.
 *
 * @author Grakn Warriors
 */
public enum GraknTxType {
    READ(0),  //Read only transaction where mutations to the graph are prohibited
    WRITE(1), //Write transaction where the graph can be mutated
    BATCH(2); //Batch transaction which enables faster writes by switching off some consitency checks

    private final int type;

    GraknTxType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return this.name();
    }

    public static GraknTxType of(int value) {
        for (GraknTxType t : GraknTxType.values()) {
            if (t.type == value) return t;
        }
        return null;
    }
}
