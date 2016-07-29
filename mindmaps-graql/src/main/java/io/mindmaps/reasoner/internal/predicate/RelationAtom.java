package io.mindmaps.reasoner.internal.predicate;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.query.*;
import io.mindmaps.reasoner.internal.container.Query;
import org.javatuples.Pair;
import sun.plugin.dom.exception.InvalidAccessException;

import java.util.*;

import static io.mindmaps.reasoner.internal.Utility.getCompatibleRoleTypes;

public class RelationAtom implements Atomic{

    private final String varName;
    private final Set<Var.Casting> castings = new HashSet<>();
    private final String typeId;

    private final Pattern.Admin atomPattern;
    private final Set<Query> expansions = new HashSet<>();

    private Query parent = null;

    public RelationAtom(Var.Admin pattern)
    {
        this.atomPattern = pattern;
        Pair<String, String> varData = getDataFromVar(atomPattern.asVar());
        varName = varData.getValue0();
        typeId = varData.getValue1();
    }

    public RelationAtom(Var.Admin pattern, Query par)
    {
        this.atomPattern = pattern;
        Pair<String, String> varData = getDataFromVar(atomPattern.asVar());
        varName = varData.getValue0();
        typeId = varData.getValue1();
        this.parent = par;
    }

    public RelationAtom(RelationAtom a)
    {
        this.atomPattern = a.atomPattern;
        Pair<String, String> varData = getDataFromVar(atomPattern.asVar());
        varName = varData.getValue0();
        typeId = varData.getValue1();
        a.expansions.forEach(exp -> expansions.add(new Query(exp)));
    }

    @Override
    public String toString(){ return atomPattern.toString(); }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof RelationAtom)) return false;
        RelationAtom a2 = (RelationAtom) obj;
        return this.getTypeId().equals(a2.getTypeId()) && this.getVarNames().equals(a2.getVarNames());
    }

    @Override
    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + getVarNames().hashCode();
        return hashCode;
    }

    @Override
    public void print()
    {
        System.out.println("atom: \npattern: " + toString());
        System.out.println("varName: " + varName + " typeId: " + typeId);
        System.out.print("Castings: ");
        castings.forEach(c -> System.out.print(c.getRolePlayer().getPrintableName() + " "));
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
    public boolean isRelation(){ return true;}
    @Override
    public boolean isValuePredicate(){ return false;}
    @Override
    public boolean isResource(){ return false;}
    @Override
    public boolean isType(){ return true;}
    @Override
    public boolean containsVar(String name)
    {
        boolean varFound = false;

        Iterator<Var.Casting> it = castings.iterator();
        while(it.hasNext() && !varFound)
            varFound = it.next().getRolePlayer().getName().equals(name);

        return varFound;
    }

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
        throw new IllegalAccessError("setVarName attempted on a relation atom!");
    }

    @Override
    public void changeRelVarName(String from, String to)
    {
        castings.stream().filter(c -> c.getRolePlayer().getName().equals(from))
                .forEach(c -> c.getRolePlayer().setName(to));
    }

    @Override
    public String getVarName(){ return varName;}
    @Override
    public Set<String> getVarNames(){
        Set<String> vars = new HashSet<>();
        getCastings().forEach(c -> vars.add(c.getRolePlayer().getName()));
        return vars;
    }
    @Override
    public String getTypeId(){ return typeId;}
    @Override
    public String getVal(){
        throw new IllegalAccessError("getVal() on a relation atom!");
    }

    @Override
    public Set<Var.Casting> getCastings(){ return castings;}

    @Override
    public Set<Query> getExpansions(){ return expansions;}


    private Pair<String, String> getDataFromVar(Var.Admin var) {

        String vTypeId = var.getType().flatMap(Var.Admin::getId).orElse("");
        String vName = "rel";
        castings.addAll(var.getCastings());

        return new Pair<>(vName, vTypeId);

    }

    /**
     * Attempts to infer the implicit roleTypes of vars in a relAtom
     * @return map containing a varName - varType, varRoleType triple
     */
    public Map<String, Pair<Type, RoleType>> getVarTypeRoleMap()
    {
        if (getParentQuery() == null)
            throw new InvalidAccessException("getVarTypeRoleMap called on atom with no parent!");

        Map<String, Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();

        String relTypeId = getTypeId();
        Set<String> vars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : vars)
        {
            Type type = varTypeMap.get(var);
            if (type != null)
            {
                Set<RoleType> cRoles = getCompatibleRoleTypes(type.getId(), relTypeId, getParentQuery().getTransaction());

                /**if roleType is unambigous*/
                if(cRoles.size() == 1)
                    roleVarTypeMap.put(var, new Pair<>(type, cRoles.iterator().next()));
                else
                    roleVarTypeMap.put(var, new Pair<>(type, null));

            }
        }
        return roleVarTypeMap;
    }

    /**
     * Attempts to infer the implicit roleTypes and matching types
     * @return map containing a RoleType-Type pair
     */
    public Map<RoleType, Pair<String, Type>> getRoleVarTypeMap()
    {
        if (getParentQuery() == null)
            throw new InvalidAccessException("getVarTypeRoleMap called on atom with no parent!");

        Map<RoleType, Pair<String, Type>> roleVarTypeMap = new HashMap<>();

        MindmapsTransaction graph =  getParentQuery().getTransaction();
        String relTypeId = getTypeId();
        Set<String> relVars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        for (String var : relVars)
        {
            Type type = varTypeMap.get(var);
            String roleTypeId = "";
            for(Var.Casting c : castings) {
                if (c.getRolePlayer().getName().equals(var))
                    roleTypeId = c.getRoleType().flatMap(Var.Admin::getId).orElse("");
            }

            /**roletype explicit*/
            if (!roleTypeId.isEmpty())
                roleVarTypeMap.put(graph.getRoleType(roleTypeId), new Pair<>(var, type));
            else {

                if (type != null) {
                    Set<RoleType> cRoles = getCompatibleRoleTypes(type.getId(), relTypeId, graph);

                    /**if roleType is unambigous*/
                    if (cRoles.size() == 1)
                        roleVarTypeMap.put(cRoles.iterator().next(), new Pair<>(var, type));

                }
            }
        }
        return roleVarTypeMap;
    }
}
