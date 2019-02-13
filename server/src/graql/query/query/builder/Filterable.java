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


package grakn.core.graql.query.query.builder;

import grakn.core.graql.query.statement.Statement;
import grakn.core.graql.query.statement.Variable;
import graql.lang.util.Token;

import java.util.Optional;

public interface Filterable {

    Optional<Sorting> sort();

    Optional<Long> offset();

    Optional<Long> limit();

    default String printFilters() {
        StringBuilder filters = new StringBuilder();

        sort().ifPresent(sort -> filters.append(Token.Filter.SORT).append(Token.Char.SPACE)
                .append(sort).append(Token.Char.SEMICOLON).append(Token.Char.SPACE));

        offset().ifPresent(offset -> filters.append(Token.Filter.OFFSET).append(Token.Char.SPACE)
                .append(offset).append(Token.Char.SEMICOLON).append(Token.Char.SPACE));

        limit().ifPresent(limit -> filters.append(Token.Filter.LIMIT).append(Token.Char.SPACE)
                .append(limit).append(Token.Char.SEMICOLON).append(Token.Char.SPACE));

        return filters.toString().trim();
    }

    interface Unfiltered<S extends Sorted, O extends Offsetted, L extends Limited> extends Filterable {

        default S sort(String var) {
            return sort(new Variable(var));
        }

        default S sort(String var, String order) {
            Token.Order o = Token.Order.of(order);
            if (o == null) throw new IllegalArgumentException(
                    "Invalid sorting order. Valid options: '" + Token.Order.ASC +"' or '" + Token.Order.DESC
            );
            return sort(new Variable(var), o);
        }

        default S sort(String var, Token.Order order) {
            return sort(new Variable(var), order);
        }

        default S sort(Variable var) {
            return sort(new Sorting(var));
        }

        default S sort(Variable var, Token.Order order) {
            return sort(new Sorting(var, order));
        }

        default S sort(Statement var) {
            return sort(new Sorting(var.var()));
        }

        default S sort(Statement var, Token.Order order) {
            return sort(new Sorting(var.var(), order));
        }

        S sort(Sorting sorting);

        O offset(long offset);

        L limit(long limit);
    }

    interface Sorted<O extends Offsetted, L extends Limited> extends Filterable {

        O offset(long offset);

        L limit(long limit);
    }

    interface Offsetted<L extends Limited> extends Filterable {

        L limit(long limit);
    }

    interface Limited extends Filterable {

    }

    class Sorting {

        private Variable var;
        private Token.Order order;

        public Sorting(Variable var) {
            this(var, null);
        }
        public Sorting(Variable var, Token.Order order) {
            this.var = var;
            this.order = order;
        }

        public Variable var() {
            return var;
        }

        public Token.Order order() {
            return order == null ? Token.Order.ASC : order;
        }

        @Override
        public String toString() {
            StringBuilder sort = new StringBuilder();

            sort.append(var);
            if (order != null) {
                sort.append(Token.Char.SPACE).append(order);
            }

            return sort.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Sorting that = (Sorting) o;

            return (this.var().equals(that.var()) &&
                    this.order().equals(that.order()));
        }

        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            h ^= this.var().hashCode();
            h *= 1000003;
            h ^= this.order().hashCode();
            return h;
        }
    }
}
