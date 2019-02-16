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

package graql.lang.query.builder;

import graql.lang.query.GraqlQuery;
import graql.lang.statement.Variable;
import graql.lang.util.Token;

public interface Aggregatable<T extends GraqlQuery> {

    default T count() {
        return aggregate(Token.Aggregate.Method.COUNT, null);
    }

    default T max(String var) {
        return max(new Variable(var));
    }

    default T max(Variable var) {
        return aggregate(Token.Aggregate.Method.MAX, var);
    }

    default T mean(String var) {
        return mean(new Variable(var));
    }

    default T mean(Variable var) {
        return aggregate(Token.Aggregate.Method.MEAN, var);
    }

    default T median(String var) {
        return median(new Variable(var));
    }

    default T median(Variable var) {
        return aggregate(Token.Aggregate.Method.MEDIAN, var);
    }

    default T min(String var) {
        return min(new Variable(var));
    }

    default T min(Variable var) {
        return aggregate(Token.Aggregate.Method.MIN, var);
    }

    default T std(String var) {
        return std(new Variable(var));
    }

    default T std(Variable var) {
        return aggregate(Token.Aggregate.Method.STD, var);
    }

    default T sum(String var) {
        return sum(new Variable(var));
    }

    default T sum(Variable var) {
        return aggregate(Token.Aggregate.Method.SUM, var);
    }

    // TODO: will be made "private" once we upgrade to Java 9 or higher
    T aggregate(Token.Aggregate.Method method, Variable var);
}
