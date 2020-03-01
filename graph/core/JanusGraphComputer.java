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

package grakn.core.graph.core;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;


public interface JanusGraphComputer extends GraphComputer {

    enum ResultMode {
        NONE, PERSIST, LOCALTX;

        public ResultGraph toResultGraph() {
            switch(this) {
                case NONE: return ResultGraph.ORIGINAL;
                case PERSIST: return ResultGraph.ORIGINAL;
                case LOCALTX: return ResultGraph.NEW;
                default: throw new AssertionError("Unrecognized option: " + this);
            }
        }

        public Persist toPersist() {
            switch(this) {
                case NONE: return Persist.NOTHING;
                case PERSIST: return Persist.VERTEX_PROPERTIES;
                case LOCALTX: return Persist.VERTEX_PROPERTIES;
                default: throw new AssertionError("Unrecognized option: " + this);
            }
        }

    }

    @Override
    JanusGraphComputer workers(int threads);

    default JanusGraphComputer resultMode(ResultMode mode) {
        result(mode.toResultGraph());
        persist(mode.toPersist());
        return this;
    }
}
