package io.mindmaps.reasoner.internal;


import com.google.common.collect.Lists;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.reasoner.internal.container.Query;
import io.mindmaps.reasoner.internal.predicate.Atomic;

import java.util.*;

public class Utility {

    public static void printMatchQueryResults(MatchQuery sq)
    {
        List<Map<String, Concept>> results = Lists.newArrayList(sq);
        Iterator<Map<String, Concept>> it = results.iterator();

        while( it.hasNext() )
        {
            Map<String, Concept> result = it.next();
            for (Map.Entry<String, Concept> entry : result.entrySet() ) {
                Concept concept = entry.getValue();
                System.out.print(entry.getKey() + ": " + concept.getId() + " : " + concept.getValue() + " ");
            }
            System.out.println();
        }
    }

    public static Type getRuleConclusionType(Rule rule)
    {
        Set<Type> types = new HashSet<>();
        Collection<Type> unfilteredTypes = rule.getConclusionTypes();
        for(Type type : unfilteredTypes)
            if (!type.isRoleType()) types.add(type);

        if (types.size() > 1)
            throw new IllegalArgumentException("Found more than single conclusion type!");

        return types.iterator().next();
    }

    public static Atomic getRuleConclusionAtom(Query ruleLHS, Query ruleRHS, Type type)
    {
        Set<Atomic> atoms = ruleRHS.getAtomsWithType(type);
        if (atoms.size() > 1)
            throw new IllegalArgumentException("Found more than single relevant conclusion atom!");

        Atomic atom = atoms.iterator().next();
        atom.setParentQuery(ruleLHS);
        return atom;
    }

    public static boolean isAtomRecursive(Atomic atom, MindmapsTransaction graph)
    {
        boolean atomRecursive = false;

        String typeId = atom.getTypeId();
        if (typeId.isEmpty()) return false;
        Type type = graph.getType(typeId);
        Collection<Rule> presentInConclusion = type.getRulesOfConclusion();
        Collection<Rule> presentInHypothesis = type.getRulesOfHypothesis();

        for(Rule rule : presentInConclusion)
            atomRecursive |= presentInHypothesis.contains(rule);

        return atomRecursive;
    }

    public static  boolean isRuleRecursive(Rule rule)
    {
        boolean ruleRecursive = false;

        Type RHStype = getRuleConclusionType(rule);
        if (rule.getHypothesisTypes().contains(RHStype) )
            ruleRecursive = true;

        return ruleRecursive;
    }

    public static Set<RoleType> getCompatibleRoleTypes(String typeId, String relId, MindmapsTransaction graph)
    {
        Set<RoleType> cRoles = new HashSet<>();

        Collection<RoleType> typeRoles = graph.getType(typeId).playsRoles();
        Collection<RoleType> relRoles = graph.getRelationType(relId).hasRoles();
        relRoles.stream().filter(typeRoles::contains).forEach(cRoles::add);
        return cRoles;
    }

    public static boolean checkAtomsCompatible(Atomic a, Atomic b, MindmapsTransaction graph)
    {
        if (!(a.isType() && b.isType()) || (a.isRelation() || b.isRelation())
           || !a.getVarName().equals(b.getVarName()) || a.isResource() || b.isResource()) return true;
        String aTypeId = a.getTypeId();
        Type aType = graph.getType(aTypeId);
        String bTypeId = b.getTypeId();
        Type bType = graph.getType(bTypeId);

        return checkTypesCompatible(aType, bType) && (a.getVal().isEmpty() || b.getVal().isEmpty() || a.getVal().equals(b.getVal()) );
    }


    public static boolean checkTypesCompatible(Type aType, Type bType) {
        return aType.equals(bType) || aType.subTypes().contains(bType) || bType.subTypes().contains(aType);
    }
}
