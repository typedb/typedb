/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.query.query;

import graql.lang.exception.GraqlException;

/**
 * A Graql query of any kind. May read and write to the graph.
 *
 * TODO: this class should return more informative exception messages
 */
public abstract class GraqlQuery {

    public GraqlDefine asGraqlDefine() {
        if (this instanceof GraqlDefine) {
            return (GraqlDefine) this;
        } else {
            throw GraqlException.create("This is not a GraqlDefine query");
        }
    }

    public GraqlUndefine asGraqlUndefine() {
        if (this instanceof GraqlUndefine) {
            return (GraqlUndefine) this;
        } else {
            throw GraqlException.create("This is not a GraqlUndefine query");
        }
    }

    public GraqlInsert asGraqlInsert() {
        if (this instanceof GraqlInsert) {
            return (GraqlInsert) this;
        } else {
            throw GraqlException.create("This is not a GraqlInsert query");
        }
    }

    public GraqlDelete asGraqlDelete() {
        if (this instanceof GraqlDelete) {
            return (GraqlDelete) this;
        } else {
            throw GraqlException.create("This is not a GraqlDelete query");
        }
    }

    public GraqlGet asGraqlGet() {
        if (this instanceof GraqlGet) {
            return (GraqlGet) this;
        } else {
            throw GraqlException.create("This is not a GraqlGet query");
        }
    }

    public GraqlGet.GraqlAggregate asGraqlGetAggregate() {
        if (this instanceof GraqlGet) {
            return (GraqlGet.GraqlAggregate) this;
        } else {
            throw GraqlException.create("This is not a GraqlGet.Aggregate query");
        }
    }

    public GraqlGroup asGraqlGetGroup() {
        if (this instanceof GraqlGroup) {
            return (GraqlGroup) this;
        } else {
            throw GraqlException.create("This is not a GraqlGet.Group query");
        }
    }

    public GraqlGroup.Aggregate asGraqlGetGroupAggregate() {
        if (this instanceof GraqlGroup) {
            return (GraqlGroup.Aggregate) this;
        } else {
            throw GraqlException.create("This is not a GraqlGet.Group.Aggregate query");
        }
    }

    public GraqlCompute<?> asGraqlCompute() {
        if (this instanceof GraqlCompute<?>) {
            return (GraqlCompute<?>) this;
        } else {
            throw GraqlException.create("This is not a GraqlCompute query");
        }
    }

    @Override
    public abstract String toString();
}
