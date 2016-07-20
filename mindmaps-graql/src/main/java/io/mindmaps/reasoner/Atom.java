package io.mindmaps.reasoner;

import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.ValuePredicate;
import io.mindmaps.graql.api.query.Var;

import java.util.*;

class Atom {

    private String varName;
    private final Set<Var.Casting> castings = new HashSet<>();
    private String typeId;
    private String val;

    private final Pattern.Admin atomPattern;

    private final Set<Query> expansions = new HashSet<>();

    public Atom(Pattern.Admin pattern)
    {
        if (!pattern.isVar() )
            throw new IllegalArgumentException("Pattern is not a var: " + pattern.toString());
        this.atomPattern = pattern;
        extractDataFromVar(atomPattern.asVar());
    }

    public Atom(Atom a)
    {
        this.atomPattern = a.atomPattern;
        extractDataFromVar(atomPattern.asVar());
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
        hashCode = hashCode * 37 + this.varName.hashCode();
        hashCode = hashCode * 37 + this.val.hashCode();
        return hashCode;
    }

    public void print()
    {
        System.out.println("atom: \npattern: " + toString());
        System.out.println("varName: " + varName + " typeId: " + typeId + " val: " + val);
        if (isValuePredicate()) System.out.println("isValuePredicate");
        if (isRelation()) System.out.println("isRelation");
        System.out.print("Castings: ");
        castings.forEach(c -> System.out.print(c.getRolePlayer().getPrintableName() + " "));
        System.out.println();
    }

    public void addExpansion(Query query){ expansions.add(query);}
    public boolean isRelation(){ return atomPattern.asVar().isRelation();}
    public boolean isValuePredicate(){ return !atomPattern.asVar().getValueEqualsPredicates().isEmpty();}
    public boolean isResource(){ return !atomPattern.asVar().getResourcePredicates().isEmpty();}
    public boolean isType(){ return !typeId.isEmpty() && !isResource() && !isRelation();}
    public Pattern.Admin getPattern(){ return atomPattern;}

    public void setVarName(String var){
        varName = var;
        atomPattern.asVar().setName(var);
    }

    public void changeRelVarName(String from, String to)
    {
        castings.forEach(c ->
        {
            if (c.getRolePlayer().getName().equals(from)) c.getRolePlayer().setName(to);
        });
    }

    public String getVarName(){ return varName;}
    public Set<String> getVarNames(){
        Set<String> vars = new HashSet<>();
        if (isRelation())
            getCastings().forEach(c -> vars.add(c.getRolePlayer().getName()));
        else
            vars.add(varName);
        return vars;
    }
    public String getTypeId(){ return typeId;}
    public String getVal(){ return val;}
    private Set<Var.Casting> getCastings(){ return castings;}
    public boolean containsVar(String name)
    {
        boolean varFound = false;
        if (isRelation())
        {
            for (Var.Casting c : castings)
                if (c.getRolePlayer().getName().equals(name)) varFound = true;
        }
        else
            if (varName.equals(name)) varFound = true;

        return varFound;
    }

    public Set<Query> getExpansions(){ return expansions;}

    private void extractDataFromVar(Var.Admin var) {

        varName = var.getName();
        val = "";

        Map<Var.Admin, Set<ValuePredicate.Admin>> resourceMap = var.getResourcePredicates();

        if (var.isRelation()) {
            typeId = var.getType().flatMap(Var.Admin::getId).orElse("");
            varName = "";
            castings.addAll(var.getCastings());
            castings.forEach(c -> varName += c.getRolePlayer().getName());
        } else if (resourceMap.size() != 0) {
            if (resourceMap.size() != 1)
                throw new IllegalArgumentException("Multiple resource types in extractData");

            Map.Entry<Var.Admin, Set<ValuePredicate.Admin>> entry = resourceMap.entrySet().iterator().next();
            typeId = entry.getKey().getId().get();

            val = entry.getValue().iterator().hasNext()? entry.getValue().iterator().next().getPredicate().getValue().toString() : "";
        } else {
            typeId = var.getType().flatMap(Var.Admin::getId).orElse("");

            if ( isValuePredicate() )
            {
                Set<ValuePredicate.Admin> valuePredicates = var.admin().getValuePredicates();
                if (valuePredicates.size() != 1)
                    throw new IllegalArgumentException("More than one value predicate in extractAtomFromVar\n"
                                + atomPattern.toString());
                else
                    val = valuePredicates.iterator().next().getPredicate().getValue().toString();
                }
        }

    }
}
