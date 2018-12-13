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

package grakn.core.graql.query;

import grakn.core.graql.answer.Answer;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.server.QueryExecutor;
import grakn.core.server.Transaction;

import javax.annotation.Nullable;

/**
 * A Graql query of any kind. May read and write to the graph.
 *
 * @param <T> The result type after executing the query
 */
public interface Query<T extends Answer> {

    /**
     * @return the special type of {@link QueryExecutor}, depending on whether the query is executed on the client or
     * server side.
     */
    default QueryExecutor executor() {
        return executor(true);
    }

    default QueryExecutor executor(boolean infer) {
        if(tx() == null) throw GraqlQueryException.noTx();
        return tx().executor(infer);
    }

    /**
     * @return the transaction {@link Transaction} associated with this query
     */
    @Nullable
    Transaction tx();

    /**
     * Graql commands to determine the type of query
     */
    enum Command {
        MATCH("match"),
        COMPUTE("compute");

        private final String command;

        Command(String command) {
            this.command = command;
        }

        @Override
        public String toString() {
            return this.command;
        }

        public static Command of(String value) {
            for (Command c : Command.values()) {
                if (c.command.equals(value)) {
                    return c;
                }
            }
            return null;
        }
    }

    /**
     * Characters available for use in the Graql syntax
     */
    enum Char {
        EQUAL("="),
        SEMICOLON(";"),
        SPACE(" "),
        COMMA(","),
        COMMA_SPACE(", "),
        SQUARE_OPEN("["),
        SQUARE_CLOSE("]"),
        QUOTE("\"");

        private final String character;

        Char(String character) {
            this.character = character;
        }

        @Override
        public String toString() {
            return this.character;
        }
    }
}
