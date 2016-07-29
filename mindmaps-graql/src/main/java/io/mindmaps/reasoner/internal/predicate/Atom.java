package io.mindmaps.reasoner.internal.predicate;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.query.*;
import io.mindmaps.reasoner.internal.container.Query;
import org.javatuples.Triplet;

import java.util.*;

public class Atom implements Atomic{

    private String varName;
    private final String typeId;
    private final String val;

    private final Pattern.Admin atomPattern;
    private final Set<Query> expansions = new HashSet<>();

    private Query parent = null;

    public Atom(Var.Admin pattern)
    {
        this.atomPattern = pattern;
        Triplet<String, String, String> varData = getDataFromVar(atomPattern.asVar());
        varName = varData.getValue0();
        typeId = varData.getValue1();
        val = varData.getValue2();
    }

    public Atom(Var.Admin pattern, Query par)
    {
        this.atomPattern = pattern;
        Triplet<String, String, String> varData = getDataFromVar(atomPattern.asVar());
        varName = varData.getValue0();
        typeId = varData.getValue1();
        val = varData.getValue2();
        this.parent = par;
    }

    public Atom(Atom a)
    {
        this.atomPattern = a.atomPattern;
        Triplet<String, String, String> varData = getDataFromVar(atomPattern.asVar());
        varName = varData.getValue0();
        typeId = varData.getValue1();
        val = varData.getValue2();
        a.expansions.forEach(exp -> expansions.add(new Query(exp)));
    }

    @Override
    public String toString(){ return atomPattern.toString(); }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof Atom)) return false;
        Atom a2 = (Atom) obj;
        return this.getTypeId().equals(a2.getTypeId()) && this.getVarName().equals(a2.getVarName())
                && this.getVal().equals(a2.getVal());
    }

    @Override
    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.val.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public void print()
    {
        System.out.println("atom: \npattern: " + toString());
        System.out.println("varName: " + varName + " typeId: " + typeId + " val: " + val);
        if (isValuePredicate()) System.out.println("isValuePredicate");
        System.out.println();
    }

    @Override
    public void addExpansion(Query query){
        query.setParentAtom(this);
        expansions.add(query);
    }
    @Override
    public void removeExpansion(Query query){
        if(expansions.contains(query))
        {
            query.setParentAtom(null);
            expansions.remove(query);
        }
    }
    @Override
    public boolean isRelation(){ return false;}

    @Override
    public boolean isValuePredicate(){ return !atomPattern.asVar().getValueEqualsPredicates().isEmpty();}
    @Override
    public boolean isResource(){ return !atomPattern.asVar().getResourcePredicates().isEmpty();}
    @Override
    public boolean isType(){ return !typeId.isEmpty();}
    @Override
    public boolean containsVar(String name){ return varName.equals(name);}

    @Override
    public Pattern.Admin getPattern(){ return atomPattern;}
    @Override
    public Pattern.Admin getExpandedPattern()
    {
        Set<Pattern.Admin> expandedPattern = new HashSet<>();
        expandedPattern.add(atomPattern);
        expansions.forEach(q -> expandedPattern.add(q.getExpandedPattern()));
        return Pattern.Admin.disjunction(expandedPattern);
    }

    @Override
    public MatchQuery getExpandedMatchQuery(MindmapsTransaction graph)
    {
        QueryBuilder qb = QueryBuilder.build(graph);
        Set<String> selectVars = isRelation()? getVarNames() : Sets.newHashSet(varName);
        return qb.match(getExpandedPattern()).select(selectVars);
    }

    @Override
    public Query getParentQuery(){return parent;}
    @Override
    public void setParentQuery(Query q){ parent = q;}

    @Override
    public void setVarName(String var){
        varName = var;
        atomPattern.asVar().setName(var);
    }

    @Override
    public void changeRelVarName(String from, String to){
        throw new IllegalAccessError("changeRelVarName attempted on a non-relation atom!");
    }

    @Override
    public String getVarName(){ return varName;}
    @Override
    public Set<String> getVarNames(){
        Set<String> vars = new HashSet<>();
        if (isRelation())
            getCastings().forEach(c -> vars.add(c.getRolePlayer().getName()));
        else
            vars.add(varName);
        return vars;
    }
    @Override
    public String getTypeId(){ return typeId;}
    @Override
    public String getVal(){ return val;}

    @Override
    public Set<Var.Casting> getCastings(){
        throw new IllegalAccessError("getCastings() attempted on a non-relation atom!");
    }

    @Override
    public Set<Query> getExpansions(){ return expansions;}

    private Triplet<String, String, String> getDataFromVar(Var.Admin var) {

        String vTypeId;
        String vName = var.getName();
        String value = "";

        Map<Var.Admin, Set<ValuePredicate.Admin>> resourceMap = var.getResourcePredicates();

        if (resourceMap.size() != 0) {
            if (resourceMap.size() != 1)
                throw new IllegalArgumentException("Multiple resource types in extractData");

            Map.Entry<Var.Admin, Set<ValuePredicate.Admin>> entry = resourceMap.entrySet().iterator().next();
            vTypeId = entry.getKey().getId().get();
            value = entry.getValue().iterator().hasNext()? entry.getValue().iterator().next().getPredicate().getValue().toString() : "";
        }
        else {
            vTypeId = var.getType().flatMap(Var.Admin::getId).orElse("");

            if ( isValuePredicate() )
            {
                Set<ValuePredicate.Admin> valuePredicates = var.admin().getValuePredicates();
                if (valuePredicates.size() != 1)
                    throw new IllegalArgumentException("More than one value predicate in extractAtomFromVar\n"
                                + atomPattern.toString());
                else
                    value = valuePredicates.iterator().next().getPredicate().getValue().toString();
                }
        }

        return new Triplet<>(vName, vTypeId, value);

    }

}

