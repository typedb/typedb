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

import grakn.core.server.Transaction;
import grakn.core.server.QueryExecutor;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.answer.Answer;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Graql query of any kind. May read and write to the graph.
 *
 * @param <T> The result type after executing the query
 */
public interface Query<T extends Answer> extends Iterable<T> {

    /**
     * @return a {@link Stream} of T, where T is a special type of {@link Answer}
     */
    @CheckReturnValue
    Stream<T> stream();

    Stream<T> stream(boolean infer);

    /**
     * @return a {@link List} of T, where T is a special type of {@link Answer}
     */
    default List<T> execute() {
        return stream().collect(Collectors.toList());
    }

    default List<T> execute(boolean infer) {
        return stream(infer).collect(Collectors.toList());
    }

    /**
     * @return an {@link Iterator} of T, where T is a special type of {@link Answer}
     */
    @Override
    @CheckReturnValue
    default Iterator<T> iterator() {
        return stream().iterator();
    }

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
     * @return boolean that indicates whether this query will modify the graph
     */
    @CheckReturnValue
    boolean isReadOnly();

    /**
     * @return the transaction {@link Transaction} associated with this query
     */
    @Nullable
    Transaction tx();

    /**
     * @return boolean that indicates whether this query will perform rule-based inference during execution
     */
    Boolean inferring();

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
