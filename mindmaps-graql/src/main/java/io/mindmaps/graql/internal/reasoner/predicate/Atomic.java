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

package io.mindmaps.graql.internal.reasoner.predicate;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.internal.reasoner.container.Query;
import javafx.util.Pair;

import java.util.Map;
import java.util.Set;

public interface Atomic {

    void print();

    void addExpansion(Query query);
    void removeExpansion(Query query);

    /**
     * @return true if the atom corresponds to a unary predicate
     * */
    default boolean isUnary(){ return false;}

    /**
     * @return true if the atom corresponds to a predicate
     * */
    default boolean isType(){ return false;}

    /**
     * @return true if the atom corresponds to a non-unary predicate
     * */
    default boolean isRelation(){return false;}

    /**
     * @return true if the atom corresponds to a value predicate (~unifier)
     * */
    default boolean isValuePredicate(){ return false;}

    /**
     * @return true if the atom corresponds to a resource predicate
     * */
    default boolean isResource(){ return false;}

    /**
     * @return true if atom alpha-equivalent
     */
    default boolean isEquivalent(Object obj){ return false;}

    default boolean isRuleResolvable(){ return false;}

    /**
     * @param name variable name
     * @return true if atom contains an occurrence of the variable name
     */
    default boolean containsVar(String name){ return false;}

    /**
     * @return the corresponding pattern
     * */
    PatternAdmin getPattern();
    /**
     * @return the corresponding pattern with all expansions
     * */

    PatternAdmin getExpandedPattern();

    /**
     *
     * @param graph graph
     * @return match query obtained by selecting free variables
     */
    MatchQuery getMatchQuery(MindmapsGraph graph);
    MatchQuery getExpandedMatchQuery(MindmapsGraph graph);

    /**
     * @return the query this atom belongs to
     * */
    Query getParentQuery();

    /**
     * @param q query this atom is supposed to belong to
     */
    void setParentQuery(Query q);

    /**
     * change each variable occurrence in the atom
     * if capture occurs it is marked with a "capture-><name of the captured occurrence>" name
     * @param from variable name to be changed
     * @param to new variable name
     */
    void changeEachVarName(String from, String to);

    /**
     * change each variable occurrence according to provided mappings
     * if capture occurs it is marked with a "capture-><name of the captured occurrence>" name
     * @param mappings contain variable mappings to be applied
     */
    void changeEachVarName(Map<String, String> mappings);

    String getVarName();
    Set<String> getVarNames();
    String getTypeId();
    String getVal();

    Set<Query> getExpansions();

    Set<Atomic> getSubstitutions();
    Set<Atomic> getTypeConstraints();
    Set<Atomic> getNeighbours();

    Map<String, Set<Atomic>> getVarSubMap();
    Map<String, Set<Atomic>> getVarConstraintMap();

    Map<String, Pair<Type, RoleType>> getVarTypeRoleMap();
    Map<RoleType, Pair<String, Type>> getRoleVarTypeMap();

}
