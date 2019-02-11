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

    public GraqlDefine asDefine() {
        if (this instanceof GraqlDefine) {
            return (GraqlDefine) this;
        } else {
            throw GraqlException.create("This is not a GraqlDefine query");
        }
    }

    public GraqlUndefine asUndefine() {
        if (this instanceof GraqlUndefine) {
            return (GraqlUndefine) this;
        } else {
            throw GraqlException.create("This is not a GraqlUndefine query");
        }
    }

    public GraqlInsert asInsert() {
        if (this instanceof GraqlInsert) {
            return (GraqlInsert) this;
        } else {
            throw GraqlException.create("This is not a GraqlInsert query");
        }
    }

    public GraqlDelete asDelete() {
        if (this instanceof GraqlDelete) {
            return (GraqlDelete) this;
        } else {
            throw GraqlException.create("This is not a GraqlDelete query");
        }
    }

    public GraqlGet asGet() {
        if (this instanceof GraqlGet) {
            return (GraqlGet) this;
        } else {
            throw GraqlException.create("This is not a GraqlGet query");
        }
    }

    public GraqlGet.Aggregate asGetAggregate() {
        if (this instanceof GraqlGet.Aggregate) {
            return (GraqlGet.Aggregate) this;
        } else {
            throw GraqlException.create("This is not a GraqlGet.Aggregate query");
        }
    }

    public GraqlGet.Group asGetGroup() {
        if (this instanceof GraqlGet.Group) {
            return (GraqlGet.Group) this;
        } else {
            throw GraqlException.create("This is not a GraqlGet.Group query");
        }
    }

    public GraqlGet.Group.Aggregate asGetGroupAggregate() {
        if (this instanceof GraqlGet.Group.Aggregate) {
            return (GraqlGet.Group.Aggregate) this;
        } else {
            throw GraqlException.create("This is not a GraqlGet.Group.Aggregate query");
        }
    }

    public GraqlCompute<?> asCompute() {
        if (this instanceof GraqlCompute<?>) {
            return (GraqlCompute<?>) this;
        } else {
            throw GraqlException.create("This is not a GraqlCompute query");
        }
    }

    @Override
    public abstract String toString();
}
