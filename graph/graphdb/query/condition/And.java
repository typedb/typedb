// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.query.condition;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;

/**
 * Combines multiple conditions under semantic AND, i.e. all conditions must be true for this combination to be true
 *
 */
public class And<E extends JanusGraphElement> extends MultiCondition<E> {

    public And(Condition<E>... elements) {
        super(elements);
    }

    public And() {
        super();
    }

    private And(org.janusgraph.graphdb.query.condition.And<E> clone) {
        super(clone);
    }

    @Override
    public org.janusgraph.graphdb.query.condition.And<E> clone() {
        return new org.janusgraph.graphdb.query.condition.And<>(this);
    }

    public And(int capacity) {
        super(capacity);
    }

    @Override
    public Type getType() {
        return Type.AND;
    }

    @Override
    public boolean evaluate(E element) {
        for (Condition<E> condition : this) {
            if (!condition.evaluate(element))
                return false;
        }

        return true;
    }

    public static <E extends JanusGraphElement> org.janusgraph.graphdb.query.condition.And<E> of(Condition<E>... elements) {
        return new org.janusgraph.graphdb.query.condition.And<>(elements);
    }

}
