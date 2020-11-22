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
 *
 */

package grakn.core.reasoner.execution.framework;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ResponseProducer {
    private final Set<List<Long>> produced;
    private final Iterator<List<Long>> traversalProducer;
    private final Set<Request> downstreamProducer;
    private Iterator<Request> downstreamProducerSelector;

    public ResponseProducer(Iterator<List<Long>> traversalProducer) {
        produced = new HashSet<>();
        this.traversalProducer = traversalProducer;
        downstreamProducer = new HashSet<>();
        downstreamProducerSelector = downstreamProducer.iterator();
    }

    public void recordProduced(List<Long> conceptMap) {
        produced.add(conceptMap);
    }

    public boolean hasProduced(List<Long> conceptMap) {
        return produced.contains(conceptMap);
    }

    public boolean hasTraversalProducer() {
        return traversalProducer.hasNext();
    }

    public Iterator<List<Long>> traversalProducer() {
        return traversalProducer;
    }

    public boolean hasDownstreamProducer() {
        return !downstreamProducer.isEmpty();
    }

    public Request nextDownstreamProducer() {
        if (!downstreamProducerSelector.hasNext()) downstreamProducerSelector = downstreamProducer.iterator();
        return downstreamProducerSelector.next();
    }

    public void addDownstreamProducer(Request request) {
        assert !(downstreamProducer.contains(request)) : "downstream answer producer already contains this request";

        downstreamProducer.add(request);
        downstreamProducerSelector = downstreamProducer.iterator();
    }

    public void removeDownstreamProducer(Request request) {
        boolean removed = downstreamProducer.remove(request);
        // only update the iterator when removing an element, to avoid resetting and reusing first request too often
        // note: this is a large performance win when processing large batches of requests
        if (removed) downstreamProducerSelector = downstreamProducer.iterator();
    }
}
