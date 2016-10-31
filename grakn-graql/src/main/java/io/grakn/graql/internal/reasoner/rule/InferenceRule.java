package io.grakn.graql.internal.reasoner.rule;

import io.grakn.MindmapsGraph;
import io.grakn.concept.Rule;
import io.grakn.concept.Type;
import io.grakn.graql.Graql;
import io.grakn.graql.QueryBuilder;
import io.grakn.graql.internal.reasoner.atom.Atom;
import io.grakn.graql.internal.reasoner.atom.Atomic;
import io.grakn.graql.internal.reasoner.query.AtomicQuery;
import io.grakn.graql.internal.reasoner.query.Query;
import io.grakn.util.ErrorMessage;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.grakn.graql.internal.reasoner.Utility.createFreshVariable;

public class InferenceRule {

    private final Query body;
    private final AtomicQuery head;

    private final Rule rule;

    public InferenceRule(Rule rl, MindmapsGraph graph){
        this.rule = rl;
        QueryBuilder qb = Graql.withGraph(graph);
        body = new Query(qb.match(qb.parsePatterns(rule.getLHS())), graph);
        head = new AtomicQuery(qb.match(qb.parsePatterns(rule.getRHS())), graph);
    }

    public Query getBody(){return body;}
    public AtomicQuery getHead(){return head;}

    private Type getRuleConclusionType() {
        Set<Type> types = new HashSet<>();
        Collection<Type> unfilteredTypes = rule.getConclusionTypes();
        types.addAll(unfilteredTypes.stream().filter(type -> !type.isRoleType()).collect(Collectors.toList()));

        if (types.size() > 1)
            throw new IllegalArgumentException(ErrorMessage.NON_HORN_RULE.getMessage(rule.getId()));

        return types.iterator().next();
    }

    public Atom getRuleConclusionAtom() {
        if (head.selectAtoms().size() > 1)
            throw new IllegalArgumentException(ErrorMessage.NON_HORN_RULE.getMessage(body.toString()));
        Atom atom = head.selectAtoms().iterator().next();
        atom.setParentQuery(body);
        return atom;
    }

    private void propagateConstraints(Atom parentAtom){
        body.addAtomConstraints(parentAtom.getIdPredicates());
        head.addAtomConstraints(body.getIdPredicates());

        if(parentAtom.isRelation() || parentAtom.isResource()) {
            head.addAtomConstraints(parentAtom.getTypeConstraints());
            body.addAtomConstraints(parentAtom.getTypeConstraints());
        }
    }

    /**
     * propagate variables to child via a relation atom (atom variables are bound)
     * @param parentAtom   parent atom (atom) being resolved (subgoal)
     */
    private void unifyViaAtom(Atomic parentAtom) {
        Atomic childAtom = getRuleConclusionAtom();
        Query parent = parentAtom.getParentQuery();
        Map<String, String> unifiers = childAtom.getUnifiers(parentAtom);

        //do alpha-conversion
        head.unify(unifiers);
        body.unify(unifiers);

        //check free variables for possible captures
        Set<String> childFVs = body.getVarSet();
        Set<String> parentBVs = parentAtom.getVarNames();
        Set<String> parentVars = parent.getVarSet();
        parentBVs.forEach(childFVs::remove);

        childFVs.forEach(chVar -> {
            // if (x e P) v (x e G)
            // x -> fresh
            if (parentVars.contains(chVar)) {
                String freshVar = createFreshVariable(body.getVarSet(), chVar);
                body.unify(chVar, freshVar);
            }
        });
    }

    /**
     * make child query consistent by performing variable IdPredicate so that parent variables are propagated
     * @param parentAtom   parent atom (atom) being resolved (subgoal)
     */
   public void unify(Atom parentAtom) {
        unifyViaAtom(parentAtom);
        propagateConstraints(parentAtom);
    }
}
