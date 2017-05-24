/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.factory;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * @author Felix Chapman
 */
public class TitanPreviousPropertyStep<S, E extends Element> extends AbstractStep<S, E> {

    private static final long serialVersionUID = -8906462828437711078L;
    private final String propertyKey;
    private final String stepLabel;

    public TitanPreviousPropertyStep(Traversal.Admin traversal, String propertyKey, String stepLabel) {
        super(traversal);
        this.propertyKey = Objects.requireNonNull(propertyKey);
        this.stepLabel = Objects.requireNonNull(stepLabel);
    }

    @Override
    protected Traverser<E> processNextStart() throws NoSuchElementException {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TitanPreviousPropertyStep<?, ?> that = (TitanPreviousPropertyStep<?, ?>) o;

        if (!propertyKey.equals(that.propertyKey)) return false;
        return stepLabel.equals(that.stepLabel);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + propertyKey.hashCode();
        result = 31 * result + stepLabel.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, propertyKey, stepLabel);
    }
}
