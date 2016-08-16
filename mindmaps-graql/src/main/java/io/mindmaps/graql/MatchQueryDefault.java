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

package io.mindmaps.graql;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.internal.AdminConverter;
import io.mindmaps.graql.internal.query.AskQueryImpl;
import io.mindmaps.graql.internal.query.DeleteQueryImpl;
import io.mindmaps.graql.internal.query.InsertQueryImpl;
import io.mindmaps.graql.internal.query.match.*;

import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * a query used for finding data in a graph that matches the given patterns.
 *
 * Each matching subgraph will produce a map, where keys are variable names and values are concepts in the graph.
 */
@SuppressWarnings("UnusedReturnValue")
public interface MatchQueryDefault extends MatchQuery<Map<String, Concept>> {
    /**
     * @param names an array of variable names to select
     * @return a new MatchQuery that selects the given variables
     */
    default MatchQueryDefault select(String... names) {
        return select(Sets.newHashSet(names));
    }

    /**
     * @param names a set of variable names to select
     * @return a new MatchQuery that selects the given variables
     */
    default MatchQueryDefault select(Set<String> names) {
        return new MatchQuerySelect(admin(), ImmutableSet.copyOf(names));
    }

    /**
     * @param name a variable name to get
     * @return a query of concepts
     */
    default MatchQuery<Concept> get(String name) {
        return new MatchQueryMap<>(admin(), result -> result.get(name));
    }

    /**
     * @return an ask query that will return true if any matches are found
     */
    default AskQuery ask() {
        return new AskQueryImpl(this);
    }

    /**
     * @param vars an array of variables to insert for each result of this match query
     * @return an insert query that will insert the given variables for each result of this match query
     */
    default InsertQuery insert(Var... vars) {
        return insert(Arrays.asList(vars));
    }

    /**
     * @param vars a collection of variables to insert for each result of this match query
     * @return an insert query that will insert the given variables for each result of this match query
     */
    default InsertQuery insert(Collection<? extends Var> vars) {
        ImmutableSet<Var.Admin> varAdmins = ImmutableSet.copyOf(AdminConverter.getVarAdmins(vars));
        return new InsertQueryImpl(varAdmins, admin());
    }

    /**
     * @param names an array of variable names to delete for each result of this match query
     * @return a delete query that will delete the given variable names for each result of this match query
     */
    default DeleteQuery delete(String... names) {
        List<Var> deleters = Arrays.stream(names).map(Graql::var).collect(toList());
        return delete(deleters);
    }

    /**
     * @param deleters an array of variables stating what properties to delete for each result of this match query
     * @return a delete query that will delete the given properties for each result of this match query
     */
    default DeleteQuery delete(Var... deleters) {
        return delete(Arrays.asList(deleters));
    }

    /**
     * @param deleters a collection of variables stating what properties to delete for each result of this match query
     * @return a delete query that will delete the given properties for each result of this match query
     */
    default DeleteQuery delete(Collection<? extends Var> deleters) {
        return new DeleteQueryImpl(AdminConverter.getVarAdmins(deleters), this);
    }

    /**
     * Order the results by degree in ascending order
     * @param varName the variable name to order the results by
     * @return a new MatchQuery with the given ordering
     */
    default MatchQueryDefault orderBy(String varName) {
        return orderBy(varName, true);
    }

    /**
     * Order the results by degree
     * @param varName the variable name to order the results by
     * @param asc whether to use ascending order
     * @return a new MatchQuery with the given ordering
     */
    default MatchQueryDefault orderBy(String varName, boolean asc) {
        return new MatchQueryOrder(admin(), new MatchOrder(varName, Optional.empty(), asc));
    }

    /**
     * Order the results by a resource in ascending order
     * @param varName the variable name to order the results by
     * @param resourceType the resource type attached to the variable to use for ordering
     * @return a new MatchQuery with the given ordering
     */
    default MatchQueryDefault orderBy(String varName, String resourceType) {
        return orderBy(varName, resourceType, true);
    }

    /**
     * Order the results by a resource
     * @param varName the variable name to order the results by
     * @param resourceType the resource type attached to the variable to use for ordering
     * @param asc whether to use ascending order
     * @return a new MatchQuery with the given ordering
     */
    default MatchQueryDefault orderBy(String varName, String resourceType, boolean asc) {
        return new MatchQueryOrder(admin(), new MatchOrder(varName, Optional.of(resourceType), asc));
    }


    @Override
    default MatchQueryDefault withTransaction(MindmapsTransaction transaction) {
        return new MatchQueryWrapper(MatchQuery.super.withTransaction(transaction).admin(), admin());
    }

    @Override
    default MatchQueryDefault limit(long limit) {
        return new MatchQueryWrapper(MatchQuery.super.limit(limit).admin(), admin());
    }

    @Override
    default MatchQueryDefault offset(long offset) {
        return new MatchQueryWrapper(MatchQuery.super.offset(offset).admin(), admin());
    }

    @Override
    default MatchQueryDefault distinct() {
        return new MatchQueryWrapper(MatchQuery.super.distinct().admin(), admin());
    }

    /**
     * @return admin instance for inspecting and manipulating this query
     */
    Admin admin();

    /**
     * Admin class for inspecting and manipulating a MatchQuery
     */
    interface Admin extends MatchQueryDefault, MatchQuery.Admin<Map<String, Concept>> {

        @Override
        default MatchQueryDefault.Admin admin() {
            return this;
        }

        /**
         * @return all selected variable names in the query
         */
        Set<String> getSelectedNames();
    }
}
