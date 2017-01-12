package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.ConceptId;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.Query;

import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

/**
 *
 * <p>
 * Base implementation for binary atoms with single predicate.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Binary extends BinaryBase {
    private IdPredicate predicate = null;

    protected Binary(VarAdmin pattern, IdPredicate p, Query par) {
        super(pattern, par);
        this.predicate = p;
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    protected Binary(Binary a) {
        super(a);
        this.predicate = a.getPredicate() != null ? (IdPredicate) AtomicFactory.create(a.getPredicate(), getParentQuery()) : null;
        this.typeId = a.getTypeId() != null? ConceptId.of(a.getTypeId().getValue()) : null;
    }

    protected abstract ConceptId extractTypeId(VarAdmin var);

    @Override
    public PatternAdmin getCombinedPattern() {
        Set<VarAdmin> vars = Sets.newHashSet(super.getPattern().asVar());
        if (getPredicate() != null) vars.add(getPredicate().getPattern().asVar());
        return Patterns.conjunction(vars);
    }

    @Override
    public void setParentQuery(Query q) {
        super.setParentQuery(q);
        if (predicate != null) predicate.setParentQuery(q);
    }

    public IdPredicate getPredicate() { return predicate;}
    protected void setPredicate(IdPredicate p) { predicate = p;}

    @Override
    protected boolean predicatesEquivalent(BinaryBase atom) {
        Predicate pred = getPredicate();
        Predicate objPredicate = ((Binary) atom).getPredicate();
        return (pred == null && objPredicate == null)
                || (pred != null  && pred.isEquivalent(objPredicate));
    }

    @Override
    public int equivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (typeId != null? this.typeId.hashCode() : 0);
        hashCode = hashCode * 37 + (predicate != null ? predicate.equivalenceHashCode() : 0);
        return hashCode;
    }

    @Override
    public boolean isValueUserDefinedName() {
        return predicate == null && !getValueVariable().getValue().isEmpty();
    }

    @Override
    public void unify (Map<VarName, VarName> unifiers) {
        super.unify(unifiers);
        if (predicate != null) predicate.unify(unifiers);
    }
}
