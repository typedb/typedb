/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
 */

package ai.grakn.graql.internal.query.analytics;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.PathsQuery;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static ai.grakn.graql.internal.util.StringConverter.idToString;

class PathQueryImpl extends AbstractComputeQuery<Optional<List<Concept>>> implements PathQuery {

    private ConceptId sourceId = null;
    private ConceptId destinationId = null;

    PathQueryImpl(Optional<GraknTx> graph) {
        this.tx = graph;
    }

    @Override
    public Optional<List<Concept>> execute() {
        PathsQuery pathsQuery = new PathsQueryImpl(tx);
        if (includeAttribute) pathsQuery = pathsQuery.includeAttribute();
        return pathsQuery.from(sourceId).to(destinationId).in(subLabels).execute().stream().findAny();
    }

    @Override
    public PathQuery from(ConceptId sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    @Override
    public PathQuery to(ConceptId destinationId) {
        this.destinationId = destinationId;
        return this;
    }

    @Override
    public PathQuery includeAttribute() {
        return (PathQuery) super.includeAttribute();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public PathQuery in(String... subTypeLabels) {
        return (PathQuery) super.in(subTypeLabels);
    }

    @Override
    public PathQuery in(Collection<Label> subLabels) {
        return (PathQuery) super.in(subLabels);
    }

    @Override
    String graqlString() {
        return "path from " + idToString(sourceId) + " to " + idToString(destinationId) + subtypeString();
    }

    @Override
    public PathQuery withTx(GraknTx tx) {
        return (PathQuery) super.withTx(tx);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PathQueryImpl pathQuery = (PathQueryImpl) o;

        return sourceId.equals(pathQuery.sourceId) && destinationId.equals(pathQuery.destinationId);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + sourceId.hashCode();
        result = 31 * result + destinationId.hashCode();
        return result;
    }
}
