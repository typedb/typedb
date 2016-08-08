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

package io.mindmaps.reasoner.internal.predicate;

import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.graql.api.query.*;
import io.mindmaps.reasoner.internal.container.Query;

import java.util.Map;
import java.util.Set;

public interface Atomic {

    void print();

    void addExpansion(Query query);
    void removeExpansion(Query query);

    default boolean isRelation(){return false;}
    default boolean isValuePredicate(){ return false;}
    default boolean isResource(){ return false;}
    default boolean isType(){ return false;}
    default boolean containsVar(String name){ return false;}

    Pattern.Admin getPattern();
    Pattern.Admin getExpandedPattern();
    MatchQuery getExpandedMatchQuery(MindmapsTransaction graph);

    Query getParentQuery();
    void setParentQuery(Query q);

    void changeEachVarName(String from, String to);
    void changeEachVarName(Map<String, String> mappings);

    String getVarName();
    Set<String> getVarNames();
    String getTypeId();
    String getVal();

    Set<Query> getExpansions();

}
