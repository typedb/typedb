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
import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.property.AbstractProperty;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.graql.query.pattern.property.ThenProperty;
import grakn.core.graql.query.pattern.property.TypeProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.WhenProperty;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

/**
 * Type Statement Properties
 */
interface StatementTypeBuilder {

    @CheckReturnValue // TODO: make this "private" once we upgrade to Java 9
    StatementType statementType(VarProperty property);
    
    /**
     * @param name a string that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    default StatementType type(String name) {
        return statementType(new TypeProperty(name));
    }

    /**
     * set this concept type variable as abstract, meaning it cannot have direct instances
     *
     * @return this
     */
    @CheckReturnValue
    default StatementType isAbstract() {
        return statementType(AbstractProperty.get());
    }

    /**
     * @param type a concept type id that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    default StatementType sub(String type) {
        return sub(Graql.type(type));
    }

    /**
     * @param type a concept type that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    default StatementType sub(Statement type) {
        return sub(new SubProperty(type));
    }

    /**
     * @param type a concept type id that this variable must be a kind of, without looking at parent types
     * @return this
     */
    @CheckReturnValue
    default StatementType subX(String type) {
        return subX(Graql.type(type));
    }

    /**
     * @param type a concept type that this variable must be a kind of, without looking at parent type
     * @return this
     */
    @CheckReturnValue
    default StatementType subX(Statement type) {
        return sub(new SubProperty(type, true));
    }

    @CheckReturnValue
    default StatementType sub(SubProperty property) {
        return statementType(property);
    }

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    default StatementType key(String type) {
        return key(Graql.var().type(type));
    }

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    default StatementType key(Statement type) {
        return statementType(new HasAttributeTypeProperty(type, true));
    }

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    default StatementType has(String type) {
        return has(Graql.type(type));
    }

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    default StatementType has(Statement type) {
        return statementType(new HasAttributeTypeProperty(type, false));
    }

    /**
     * @param type a Role id that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    default StatementType plays(String type) {
        return plays(Graql.type(type));
    }

    /**
     * @param type a Role that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    default StatementType plays(Statement type) {
        return statementType(new PlaysProperty(type, false));
    }

    /**
     * @param type a Role id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    default StatementType relates(String type) {
        return relates(type, null);
    }

    /**
     * @param type a Role that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    default StatementType relates(Statement type) {
        return relates(type, null);
    }

    /**
     * @param roleType a Role id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    default StatementType relates(String roleType, @Nullable String superRoleType) {
        return relates(Graql.type(roleType), superRoleType == null ? null : Graql.type(superRoleType));
    }

    /**
     * @param roleType a Role that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    default StatementType relates(Statement roleType, @Nullable Statement superRoleType) {
        return statementType(new RelatesProperty(roleType, superRoleType));
    }

    /**
     * @param datatype the datatype to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    default StatementType datatype(String datatype) {
        return datatype(Query.DataType.of(datatype));
    }

    /**
     * @param datatype the datatype to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    default StatementType datatype(Query.DataType datatype) {
        return statementType(new DataTypeProperty(datatype));
    }

    /**
     * Specify the regular expression instances of this resource type must match
     *
     * @param regex the regex to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    default StatementType regex(String regex) {
        return statementType(new RegexProperty(regex));
    }

    /**
     * @param when the left-hand side of this rule
     * @return this
     */
    @CheckReturnValue // TODO: make when() method take a more strict sub type of pattern
    default StatementType when(Pattern when) {
        return statementType(new WhenProperty(when));
    }

    /**
     * @param then the right-hand side of this rule
     * @return this
     */
    @CheckReturnValue // TODO: make then() method take a more strict sub type of pattern
    default StatementType then(Pattern then) {
        return statementType(new ThenProperty(then));
    }
}
