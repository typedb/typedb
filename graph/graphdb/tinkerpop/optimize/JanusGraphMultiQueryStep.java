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

package grakn.core.graph.graphdb.tinkerpop.optimize;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * This step can be injected before a traversal parent, such as a union, and will cache the
 * starts sent to the parent. The traversal parent will drip feed those starts into its
 * child traversal. If the initial step of that child supports multiQuery then its faster
 * if initialised with all the starts than just one at a time, so this step allows it to
 * request the full set of starts from this step when initialising itself.
 */
public final class JanusGraphMultiQueryStep extends AbstractStep<Vertex, Vertex> {

    private final Set<Admin<Vertex>> cachedStarts = new HashSet<Admin<Vertex>>();
    private final String forStep;
    private boolean cachedStartsAccessed = false;

    public JanusGraphMultiQueryStep(Step<Vertex,?> originalStep) {
        super(originalStep.getTraversal());
        this.forStep = originalStep.getClass().getSimpleName();
    }

    @Override
    protected Admin<Vertex> processNextStart() throws NoSuchElementException {
        Admin<Vertex> start = this.starts.next();
        if (!cachedStarts.contains(start))
        {
            if (cachedStartsAccessed) {
                cachedStarts.clear();
                cachedStartsAccessed = false;
            }
            final List<Admin<Vertex>> newStarters = new ArrayList<>();
            starts.forEachRemaining(s -> {
                newStarters.add(s);
                cachedStarts.add(s);
            });
            starts.add(newStarters.iterator());
            cachedStarts.add(start);
        }
        return start;
    }

    public List<Admin<Vertex>> getCachedStarts() {
        cachedStartsAccessed = true;
        return Lists.newArrayList(cachedStarts);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, forStep);
    }

    @Override
    public void reset() {
        super.reset();
        this.cachedStarts.clear();
    }
}
