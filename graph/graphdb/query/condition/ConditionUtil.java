/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.query.condition;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphElement;

import javax.annotation.Nullable;
import java.util.function.Predicate;

/**
 * Utility methods for transforming and inspecting Conditions.
 */
public class ConditionUtil {

    public static <E extends JanusGraphElement> Condition<E> literalTransformation(Condition<E> condition, Function<Condition<E>, Condition<E>> transformation) {
        return transformation(condition, new Function<Condition<E>, Condition<E>>() {
            @Nullable
            @Override
            public Condition<E> apply(Condition<E> cond) {
                if (cond.getType() == Condition.Type.LITERAL) return transformation.apply(cond);
                else return null;
            }
        });
    }

    public static <E extends JanusGraphElement> Condition<E> transformation(Condition<E> condition, Function<Condition<E>, Condition<E>> transformation) {
        Condition<E> transformed = transformation.apply(condition);
        if (transformed != null) return transformed;
        //if transformed==null we go a level deeper
        if (condition.getType() == Condition.Type.LITERAL) {
            return condition;
        } else if (condition instanceof Not) {
            return Not.of(transformation(((Not) condition).getChild(), transformation));
        } else if (condition instanceof And) {
            final And<E> newAnd = new And<>(condition.numChildren());
            for (Condition<E> child : condition.getChildren()) newAnd.add(transformation(child, transformation));
            return newAnd;
        } else if (condition instanceof Or) {
            final Or<E> newOr = new Or<>(condition.numChildren());
            for (Condition<E> child : condition.getChildren()) newOr.add(transformation(child, transformation));
            return newOr;
        } else throw new IllegalArgumentException("Unexpected condition type: " + condition);
    }

    public static <E extends JanusGraphElement> void traversal(Condition<E> condition, Predicate<Condition<E>> evaluator) {
        Preconditions.checkArgument(!evaluator.test(condition)
                || condition.getType() == Condition.Type.LITERAL
                || condition instanceof Not
                || condition instanceof MultiCondition, "Unexpected condition type: " + condition);
        if (condition instanceof Not) {
            traversal(((Not) condition).getChild(), evaluator);
        } else if (condition instanceof MultiCondition) {
            condition.getChildren().forEach(child -> traversal(child, evaluator));
        }
    }

}
