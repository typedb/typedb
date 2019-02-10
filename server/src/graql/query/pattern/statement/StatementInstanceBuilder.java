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

package grakn.core.graql.query.pattern.statement;

import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import javax.annotation.CheckReturnValue;
import java.time.LocalDateTime;

interface StatementInstanceBuilder {

    @Deprecated         // This method should not be used publicly
    @CheckReturnValue   // TODO: will be made "private" once we upgrade to Java 9
    StatementInstance statementInstance(VarProperty property);
    /**
     * @param type a concept type id that the variable must be of this type directly or indirectly
     * @return this
     */
    @CheckReturnValue
    default StatementInstance isa(String type) {
        return isa(Graql.type(type));
    }

    /**
     * @param type a concept type that this variable must be an instance of directly or indirectly
     * @return this
     */
    @CheckReturnValue
    default StatementInstance isa(Statement type) {
        return isa(new IsaProperty(type));
    }

    /**
     * @param type a concept type id that the variable must be of this type directly
     * @return this
     */
    @CheckReturnValue
    default StatementInstance isaX(String type) {
        return isaX(Graql.type(type));
    }

    /**
     * @param type a concept type that this variable must be an instance of directly
     * @return this
     */
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
}
