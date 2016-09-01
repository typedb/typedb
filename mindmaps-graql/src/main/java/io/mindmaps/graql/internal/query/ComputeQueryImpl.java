/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query;

import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.graql.ComputeQuery;
import io.mindmaps.graql.internal.analytics.Analytics;

import java.util.concurrent.ExecutionException;

public class ComputeQueryImpl implements ComputeQuery {

    String computeMethod;

    public ComputeQueryImpl(String computeMethod) {
        this.computeMethod = computeMethod;
    }

    @Override
    public Object execute(MindmapsGraph graph) throws ExecutionException, InterruptedException {
        Analytics analytics = new Analytics(graph.getName());
        switch (computeMethod) {
            case "count": {
                return analytics.count();
            }
            case "degrees": {
                return analytics.degrees();
            }
            case "degreesAndPersist": {
                analytics.degreesAndPersist();
                return "Degrees have been persisted.";
            }
            default: {
                throw new RuntimeException(ErrorMessage.NO_ANALYTICS_METHOD.getMessage(computeMethod));
            }
        }
    }

    @Override
    public String toString() {
        return "compute "+computeMethod+"()";
    }

}
