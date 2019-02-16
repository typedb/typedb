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

package graql.lang.statement.builder;

import graql.lang.Graql;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.IsaProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementInstance;

import javax.annotation.CheckReturnValue;
import java.time.LocalDateTime;

public interface StatementInstanceBuilder {

    @CheckReturnValue
    default StatementInstance isa(String type) {
        return isa(Graql.type(type));
    }

    @CheckReturnValue
    default StatementInstance isa(Statement type) {
        return isa(new IsaProperty(type));
    }

    @CheckReturnValue
    default StatementInstance isaX(String type) {
        return isaX(Graql.type(type));
    }

    @CheckReturnValue
    default StatementInstance isaX(Statement type) {
        return isa(new IsaProperty(type, true));
    }

    @CheckReturnValue
    default StatementInstance isa(IsaProperty property) {
        return statementInstance(property);
    }

    @CheckReturnValue
    default StatementInstance has(String type, long value) {
        return has(type, Graql.val(value));
    }

    @CheckReturnValue
    default StatementInstance has(String type, long value, Statement via) {
        return has(type, Graql.val(value), via);
    }

    @CheckReturnValue
    default StatementInstance has(String type, double value) {
        return has(type, Graql.val(value));
    }

    @CheckReturnValue
    default StatementInstance has(String type, double value, Statement via) {
        return has(type, Graql.val(value), via);
    }

    @CheckReturnValue
    default StatementInstance has(String type, boolean value) {
        return has(type, Graql.val(value));
    }

    @CheckReturnValue
    default StatementInstance has(String type, boolean value, Statement via) {
        return has(type, Graql.val(value), via);
    }

    @CheckReturnValue
    default StatementInstance has(String type, String value) {
        return has(type, Graql.val(value));
    }

    @CheckReturnValue
    default StatementInstance has(String type, String value, Statement via) {
        return has(type, Graql.val(value), via);
    }

    @CheckReturnValue
    default StatementInstance has(String type, LocalDateTime value) {
        return has(type, Graql.val(value));
    }

    @CheckReturnValue
    default StatementInstance has(String type, LocalDateTime value, Statement via) {
        return has(type, Graql.val(value), via);
    }

    @CheckReturnValue
    default StatementInstance has(String type, Statement variable) {
        return has(new HasAttributeProperty(type, variable));
    }

    @CheckReturnValue
    default StatementInstance has(String type, Statement variable, Statement via) {
        return has(new HasAttributeProperty(type, variable, via));
    }

    @CheckReturnValue
    default StatementInstance has(HasAttributeProperty property) {
        return statementInstance(property);
    }

    @CheckReturnValue   // TODO: will be made "private" once we upgrade to Java 9 or higher
    StatementInstance statementInstance(VarProperty property);
}
