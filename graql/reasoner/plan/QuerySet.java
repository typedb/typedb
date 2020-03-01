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

package grakn.core.graql.reasoner.plan;

import com.google.common.base.Equivalence;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Helper class for sets of ReasonerQueryImpl queries with equality comparison ReasonerQueryEquivalence.
 * </p>
 *
 *
 */
public class QuerySet extends QueryCollection<Set<ReasonerQueryImpl>, Set<Equivalence.Wrapper<ReasonerQueryImpl>>> {

    private QuerySet(Collection<ReasonerQueryImpl> queries){
        this.collection = new HashSet<>(queries);
        this.wrappedCollection = queries.stream().map(q -> equality().wrap(q)).collect(Collectors.toSet());
    }

    public static QuerySet create(Collection<Equivalence.Wrapper<ReasonerQueryImpl>> queries){
        return new QuerySet(queries.stream().map(Equivalence.Wrapper::get).collect(Collectors.toSet()));
    }
}
