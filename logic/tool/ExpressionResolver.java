/*
 * Copyright (C) 2022 Vaticle
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
 *
 */

package com.vaticle.typedb.core.logic.tool;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.constraint.value.AssignmentConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.ValueVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.expression.Expression;
import com.vaticle.typedb.core.traversal.expression.ExpressionFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Expression.AMBIGUOUS_VARIABLE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Expression.FUNCTION_ARGUMENTS_INCOMPATIBLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.VALUE_ASSIGNMENT_CYCLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.VALUE_VARIABLE_UNASSIGNED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.lang.String.join;

public class ExpressionResolver {

    private final GraphManager graphMgr;

    public ExpressionResolver(GraphManager graphMgr) {
        this.graphMgr = graphMgr;
    }

    public void resolveExpressions(Disjunction disjunction) {
        disjunction.conjunctions().forEach(this::resolveExpressions);
    }

    public void resolveExpressions(Conjunction conjunction) {
        List<ValueVariable> valueVariables = iterate(conjunction.variables())
                .filter(Variable::isValue).map(Variable::asValue).toList();

        valueVariables.forEach(this::validateAssignment);
        valueVariables.forEach(v -> validateAcyclic(v, new HashSet<>(), new Stack<>()));
        valueVariables.forEach(this::resolveAssignment);
    }

    private void validateAssignment(ValueVariable valueVariable) {
        if (valueVariable.assignment() == null) {
            throw TypeDBException.of(VALUE_VARIABLE_UNASSIGNED, valueVariable);
        }
    }

    private void validateAcyclic(ValueVariable var, Set<ValueVariable> validated, Stack<ValueVariable> stack) {
        if (validated.contains(var)) return;
        // TODO: optimise this data structure - currently O(n) contains check
        if (stack.contains(var)) {
            stack.push(var);
            throw TypeDBException.of(VALUE_ASSIGNMENT_CYCLE, stack);
        }
        stack.push(var);
        iterate(var.assignment().valueArguments()).forEachRemaining(v -> validateAcyclic(v.asValue(), validated, stack));
        ValueVariable poppedAt = stack.pop();
        assert poppedAt == var;
        validated.add(var);
    }

    public void resolveAssignment(ValueVariable variable) {
        if (variable.assignment().traversalExpression() == null) {
            Map<Identifier, Encoding.ValueType<?>> varTypes = computeArgTypes(variable.assignment());
            variable.assignment().traversalExpression(compileExpression(variable.assignment().expression(), varTypes));
        }
    }

    private Map<Identifier, Encoding.ValueType<?>> computeArgTypes(AssignmentConstraint assignmentConstraint) {
        Map<Identifier, Encoding.ValueType<?>> argumentTypes = new HashMap<>();
        assignmentConstraint.thingArguments().forEach(argument ->
                argumentTypes.put(argument.id(), getThingValueType(argument.asThing()))
        );
        assignmentConstraint.valueArguments().forEach(argument -> {
            resolveAssignment(argument);
            argumentTypes.put(argument.id(), argument.assignment().valueType());
        });
        return argumentTypes;
    }

    private Encoding.ValueType<?> getThingValueType(ThingVariable thingVar) {
        Set<? extends Encoding.ValueType<?>> valueTypes = iterate(thingVar.inferredTypes()).map(label -> graphMgr.schema().getType(label).valueType()).toSet();
        if (valueTypes.size() != 1) {
            String formatted = join(", ", iterate(valueTypes).map(Encoding.ValueType::toString).toList());
            throw TypeDBException.of(AMBIGUOUS_VARIABLE_TYPE, thingVar, formatted);
        }
        return valueTypes.iterator().next();
    }

    private Expression<?> compileExpression(
            com.vaticle.typeql.lang.pattern.expression.Expression expr,
            Map<Identifier, Encoding.ValueType<?>> varTypes
    ) {
        if (expr.isConceptVar()) {
            Identifier.Variable id = Identifier.Variable.of(expr.asConceptVar().reference().asName().asConcept());
            return ExpressionFactory.var(id, varTypes.get(id));
        } else if (expr.isValueVar()) {
            Identifier.Variable id = Identifier.Variable.of(expr.asValueVar().reference().asName().asValue());
            return ExpressionFactory.var(id, varTypes.get(id));
        } else if (expr.isConstant()) {
            com.vaticle.typeql.lang.pattern.expression.Expression.Constant<?> constant = expr.asConstant();
            return ExpressionFactory.constant(constant);
        } else if (expr.isOperation()) {
            Expression<?> first = compileExpression(expr.asOperation().operands().first(), varTypes);
            Expression<?> second = compileExpression(expr.asOperation().operands().second(), varTypes);
            return ExpressionFactory.operation(expr.asOperation().operator(), convertArgsToSingleType(expr, list(first, second)));
        } else if (expr.isFunction()) {
            List<? extends Expression<?>> args = iterate(expr.asFunction().arguments()).map(arg -> compileExpression(arg, varTypes)).toList();
            return ExpressionFactory.function(expr.asFunction().symbol(), convertArgsToSingleType(expr, args));
        } else if (expr.isParenthesis()) {
            return compileExpression(expr.asParenthesis().inner(), varTypes);
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    private <T> List<Expression<T>> convertArgsToSingleType(
            com.vaticle.typeql.lang.pattern.expression.Expression expr, List<? extends Expression<?>> exprArgs
    ) {
        assert !exprArgs.isEmpty();
        Encoding.ValueType<T> valueType = commonValueType(expr, exprArgs);
        List<Expression<T>> convertedArgs = new ArrayList<>();
        Function<Expression<?>, Expression<?>> convertFunction;
        if (valueType == Encoding.ValueType.LONG) convertFunction = ExpressionFactory::convertToLong;
        else if (valueType == Encoding.ValueType.DOUBLE) convertFunction = ExpressionFactory::convertToDouble;
        else if (valueType == Encoding.ValueType.BOOLEAN) convertFunction = ExpressionFactory::convertToBoolean;
        else if (valueType == Encoding.ValueType.STRING) convertFunction = ExpressionFactory::convertToString;
        else if (valueType == Encoding.ValueType.DATETIME) convertFunction = ExpressionFactory::convertToDateTime;
        else throw TypeDBException.of(ILLEGAL_STATE);
        exprArgs.forEach(arg -> {
            if (arg.returnType() == valueType) convertedArgs.add((Expression<T>) arg);
            else convertedArgs.add((Expression<T>) convertFunction.apply(arg));
        });
        return convertedArgs;
    }

    private <T> Encoding.ValueType<T> commonValueType(
            com.vaticle.typeql.lang.pattern.expression.Expression expr, List<? extends Expression<?>> exprArgs
    ) {
        Encoding.ValueType<?> valueType = null;
        Expression<?> valueTypeSource = null;
        for (Expression<?> arg : exprArgs) {
            if (valueType == null || valueType.assignables().contains(arg.returnType())) {
                valueType = arg.returnType();
                valueTypeSource = arg;
            } else if (valueType == arg.returnType() || arg.returnType().assignables().contains(valueType)) {
                continue;
            } else {
                throw TypeDBException.of(
                        FUNCTION_ARGUMENTS_INCOMPATIBLE, expr, valueTypeSource, valueType, arg, arg.returnType()
                );
            }
        }
        return (Encoding.ValueType<T>) valueType;
    }
}
