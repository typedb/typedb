package io.mindmaps.graql.internal.reasoner.query;

import com.google.common.collect.Sets;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;

import io.mindmaps.graql.internal.reasoner.predicate.Relation;
import io.mindmaps.graql.internal.reasoner.predicate.Substitution;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.reasoner.Utility.computeRoleCombinations;
import static io.mindmaps.graql.internal.reasoner.query.QueryAnswers.getUnifiedAnswers;

public class AtomicMatchQuery extends AtomicQuery{

    final private QueryAnswers answers;

    public AtomicMatchQuery(Atomic atom){
        super(atom);
        answers = new QueryAnswers();
    }

    public AtomicMatchQuery(AtomicQuery query, QueryAnswers ans){
        super(query);
        answers = new QueryAnswers(ans);
    }

    public AtomicMatchQuery(AtomicMatchQuery query){
        super(query);
        answers = new QueryAnswers(query.getAnswers());
    }

    @Override
    public Stream<Map<String, Concept>> stream() {
        return answers.stream();
    }

    @Override
    public QueryAnswers getAnswers(){ return answers;}

    @Override
    public void DBlookup() {
        answers.addAll(Sets.newHashSet(getMatchQuery().distinct()));
    }

    @Override
    public void memoryLookup(Map<AtomicQuery, AtomicQuery> matAnswers) {
        AtomicQuery equivalentQuery = matAnswers.get(this);
        if(equivalentQuery != null)
            answers.addAll(getUnifiedAnswers(this, equivalentQuery, equivalentQuery.getAnswers()));
    }

    @Override
    public void propagateAnswers(Map<AtomicQuery, AtomicQuery> matAnswers) {
        getChildren().forEach(childQuery -> {
            QueryAnswers ans = getUnifiedAnswers(childQuery, this, matAnswers.get(this).getAnswers());
            childQuery.getAnswers().addAll(ans);
            childQuery.propagateAnswers(matAnswers);
        });
    }

    @Override
    public QueryAnswers materialise(){
        QueryAnswers fullAnswers = new QueryAnswers();
        answers.forEach(answer -> {
            Set<Substitution> subs = new HashSet<>();
            answer.forEach((var, con) -> {
                Substitution sub = new Substitution(var, con);
                if (!containsAtom(sub))
                    subs.add(sub);
            });
            fullAnswers.addAll(materialise(subs));
        });
        return fullAnswers;
    }

    /*
    static public void recordAnswers(AtomicQuery atomicQuery, Map<AtomicQuery, AtomicQuery> matAnswers) {
        AtomicQuery equivalentQuery = matAnswers.get(atomicQuery);
        if (equivalentQuery != null) {
            QueryAnswers unifiedAnswers = getUnifiedAnswers(equivalentQuery, atomicQuery, atomicQuery.getAnswers());
            matAnswers.get(atomicQuery).getAnswers().addAll(unifiedAnswers);
        }
        else
            matAnswers.put(atomicQuery, atomicQuery);
    }

    @Override
    public void record(AtomicQuery ruleHead, Map<AtomicQuery, AtomicQuery> matAnswers) {
        Atomic headAtom = ruleHead.getAtom();
        Atomic queryAtom = getAtom();

        if(headAtom.isRelation() ){
            //if rule head generalises query atom -> extrapolate head and save constituent queries
            if ( (headAtom.getRoleVarTypeMap().size() < queryAtom.getRoleVarTypeMap().size() )) {
                String relTypeId = headAtom.getTypeId();
                RelationType relType = graph.getRelationType(relTypeId);
                Set<String> vars = headAtom.getVarNames();
                Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

                Set<Map<String, String>> roleMaps = new HashSet<>();
                computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

                roleMaps.forEach(map -> {
                    AtomicMatchQuery extrapolatedQuery = new AtomicMatchQuery(this, ruleHead.getAnswers());
                    Relation relationWithRoles = new Relation(relTypeId, map, extrapolatedQuery);
                    extrapolatedQuery.removeAtom(extrapolatedQuery.getAtom());
                    extrapolatedQuery.addAtom(relationWithRoles);
                    recordAnswers(extrapolatedQuery, matAnswers);
                    if (extrapolatedQuery.equals(this))
                        answers.addAll(ruleHead.getAnswers());
                });
            }
            //if rule head specializes query atom -> save head answers to query
            else {
                answers.addAll(ruleHead.getAnswers());
                recordAnswers(this, matAnswers);
            }
        }
        else {
            answers.addAll(ruleHead.getAnswers());
            recordAnswers(this, matAnswers);
        }

    }
    */
}
