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

package grakn.core.graph.graphdb.tinkerpop.profile;

import com.google.common.base.Preconditions;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;

public class TP3ProfileWrapper implements QueryProfiler {

    private final MutableMetrics metrics;
    private int subMetricCounter = 0;

    public TP3ProfileWrapper(MutableMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public QueryProfiler addNested(String groupName) {
        //Flatten out AND/OR nesting
        if (groupName.equals(AND_QUERY) || groupName.equals(OR_QUERY)) return this;

        int nextId = (subMetricCounter++);
        MutableMetrics nested = new MutableMetrics(metrics.getId() + "." + groupName + "_" + nextId, groupName);
        metrics.addNested(nested);
        return new TP3ProfileWrapper(nested);
    }

    @Override
    public QueryProfiler setAnnotation(String key, Object value) {
        Preconditions.checkNotNull(key, "Key must be not null");
        Preconditions.checkNotNull(value, "Value must be not null");
        if (!(value instanceof String) && !(value instanceof Number)) value = value.toString();
        metrics.setAnnotation(key, value);
        return this;
    }

    @Override
    public void startTimer() {
        metrics.start();
    }

    @Override
    public void stopTimer() {
        metrics.stop();
    }

    @Override
    public void setResultSize(long size) {
        metrics.incrementCount(TraversalMetrics.ELEMENT_COUNT_ID, size);
    }
}
