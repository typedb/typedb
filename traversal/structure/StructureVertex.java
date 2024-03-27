/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.structure;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class StructureVertex<PROPERTY extends TraversalVertex.Properties>
        extends TraversalVertex<StructureEdge<?, ?>, PROPERTY> {

    StructureVertex(Identifier identifier) {
        super(identifier);
    }

    public StructureVertex.Thing asThing() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(StructureVertex.Thing.class));
    }

    public StructureVertex.Type asType() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(StructureVertex.Type.class));
    }

    public Value asValue() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Value.class));
    }

    public static class Thing extends StructureVertex<TraversalVertex.Properties.Thing> {

        Thing(Identifier identifier) {
            super(identifier);
        }

        @Override
        protected Properties.Thing newProperties() {
            return new Properties.Thing();
        }

        @Override
        public boolean isThing() {
            return true;
        }

        @Override
        public StructureVertex.Thing asThing() {
            return this;
        }
    }

    public static class Type extends StructureVertex<Properties.Type> {

        Type(Identifier identifier) {
            super(identifier);
        }

        @Override
        protected Properties.Type newProperties() {
            return new Properties.Type();
        }

        @Override
        public boolean isType() {
            return true;
        }

        @Override
        public StructureVertex.Type asType() {
            return this;
        }
    }

    public static class Value extends StructureVertex<Properties.Value> {

        Value(Identifier identifier) {
            super(identifier);
        }

        @Override
        protected Properties.Value newProperties() {
            return new Properties.Value();
        }

        @Override
        public boolean isValue() {
            return true;
        }

        @Override
        public Value asValue() {
            return this;
        }
    }
}
