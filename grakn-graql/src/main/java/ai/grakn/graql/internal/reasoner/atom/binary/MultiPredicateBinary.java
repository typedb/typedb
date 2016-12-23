package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.Query;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 * <p>
 * Base implementation for binary atoms with multiple predicate.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class MultiPredicateBinary extends BinaryBase {
    private Set<Predicate> multiPredicate = new HashSet<>();

    protected MultiPredicateBinary(VarAdmin pattern, Set<Predicate> preds, Query par) {
        super(pattern, par);
        this.multiPredicate.addAll(preds);
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    protected MultiPredicateBinary(MultiPredicateBinary a) {
        super(a);
        a.getMultiPredicate().forEach(multiPredicate::add);
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    protected abstract String extractTypeId(VarAdmin var);

    @Override
    public void setParentQuery(Query q) {
        super.setParentQuery(q);
        multiPredicate.forEach(pred -> pred.setParentQuery(q));
    }

    public Set<Predicate> getMultiPredicate() { return multiPredicate;}

    @Override
    protected boolean predicatesEquivalent(BinaryBase at) {
        MultiPredicateBinary atom = (MultiPredicateBinary) at;
        boolean predicatesEquivalent = true;
        Iterator<Predicate> it = getMultiPredicate().iterator();
        while(it.hasNext() && predicatesEquivalent){
            Iterator<Predicate> objIt = atom.getMultiPredicate().iterator();
            boolean predicateHasEquivalent = false;
            while(objIt.hasNext() && !predicateHasEquivalent)
                predicateHasEquivalent = it.next().isEquivalent(objIt.next());
            predicatesEquivalent = predicateHasEquivalent;
        }
        return predicatesEquivalent;
    }

    private int multiPredicateEquivalenceHashCode(){
        int hashCode = 0;
        for (Predicate aMultiPredicate : multiPredicate) hashCode += aMultiPredicate.equivalenceHashCode();
        return hashCode;
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + multiPredicateEquivalenceHashCode();
        return hashCode;
    }

    @Override
    public boolean isValueUserDefinedName() { return multiPredicate.isEmpty();}

    @Override
    public void unify(String from, String to) {
        super.unify(from, to);
        multiPredicate.forEach(predicate -> predicate.unify(from, to));
    }

    @Override
    public void unify (Map<String, String> unifiers) {
        super.unify(unifiers);
        multiPredicate.forEach(predicate -> predicate.unify(unifiers));
    }
}
