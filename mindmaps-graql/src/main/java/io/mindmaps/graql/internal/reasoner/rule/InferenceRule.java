package io.mindmaps.graql.internal.reasoner.rule;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.internal.reasoner.atom.Atom;
import io.mindmaps.graql.internal.reasoner.atom.Atomic;
import io.mindmaps.graql.internal.reasoner.atom.Binary;
import io.mindmaps.graql.internal.reasoner.atom.Predicate;
import io.mindmaps.graql.internal.reasoner.query.AtomicQuery;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.util.ErrorMessage;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.reasoner.Utility.createFreshVariable;

public class InferenceRule {

    private final Query body;
    private final AtomicQuery head;

    private final Rule rule;

    public InferenceRule(Rule rl, MindmapsGraph graph){
        this.rule = rl;
        QueryBuilder qb = graph.graql();
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
        if(parentAtom.isRelation() || parentAtom.isResource()) {
            Set<Atom> types = parentAtom.getTypeConstraints().stream()
                    .filter(type -> !body.containsEquivalentAtom(type))
                    .collect(Collectors.toSet());
            Set<Predicate> predicates = new HashSet<>();
            types.stream().map(type -> (Binary) type)
                    .filter(type -> type.getPredicate() != null)
                    .map(Binary::getPredicate)
                    .forEach(predicates::add);
            head.addAtomConstraints(predicates);
            body.addAtomConstraints(predicates);
            head.addAtomConstraints(types);
            body.addAtomConstraints(types);
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
