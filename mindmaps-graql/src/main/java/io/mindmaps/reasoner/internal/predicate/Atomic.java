package io.mindmaps.reasoner.internal.predicate;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.query.*;
import io.mindmaps.reasoner.internal.container.Query;

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

    void setVarName(String var);
    void changeRelVarName(String from, String to);

    String getVarName();
    Set<String> getVarNames();
    String getTypeId();
    String getVal();
    Set<Var.Casting> getCastings();

    Set<Query> getExpansions();

}
