package ai.grakn.graql.internal.reasoner.atom;


import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.HasResourceProperty;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.util.ErrorMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class Binary extends Atom{

    private String valueVariable;

    public Binary(VarAdmin pattern) {
        super(pattern);
        this.valueVariable = extractName(pattern);
    }

    public Binary(VarAdmin pattern, Query par) {
        super(pattern, par);
        this.valueVariable = extractName(pattern);
    }

    public Binary(Binary a) {
        super(a);
        this.valueVariable = extractName(a.getPattern().asVar());
    }

    protected abstract String extractName(VarAdmin var);

    @Override
    public boolean equals(Object obj) {
        if (!(obj.getClass().equals(this.getClass()))) return false;
        Binary a2 = (Binary) obj;
        return this.typeId.equals(a2.getTypeId()) && this.varName.equals(a2.getVarName())
                && this.valueVariable.equals(a2.getValueVariable());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.valueVariable.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (!(obj.getClass().equals(this.getClass()))) return false;
        Binary a2 = (Binary) obj;
        Query parent = getParentQuery();
        return this.typeId.equals(a2.getTypeId())
                && parent.getIdPredicate(valueVariable).equals(a2.getParentQuery().getIdPredicate(a2.valueVariable));
    }

    @Override
    public int equivalenceHashCode(){
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + getParentQuery().getIdPredicate(this.valueVariable).hashCode();
        return hashCode;
    }

    public String getValueVariable(){ return valueVariable;}

    private void setValueVariable(String var){
        valueVariable = var;
        atomPattern.asVar().getProperties(HasResourceProperty.class).forEach(prop -> prop.getResource().setName(var));
    }

    @Override
    public Set<String> getVarNames() {
        Set<String> varNames = Sets.newHashSet(getVarName());
        String valueVariable = extractName(getPattern().asVar());
        if (!valueVariable.isEmpty()) varNames.add(valueVariable);
        return varNames;
    }

    @Override
    public void unify(String from, String to) {
        super.unify(from, to);
        String var = valueVariable;
        if (var.equals(from)) {
            setValueVariable(to);
        } else if (var.equals(to)) {
            setValueVariable("captured->" + var);
        }
    }

    @Override
    public void unify (Map<String, String> unifiers) {
        super.unify(unifiers);
        String var = valueVariable;
        if (unifiers.containsKey(var)) {
            setValueVariable(unifiers.get(var));
        }
        else if (unifiers.containsValue(var)) {
            setValueVariable("captured->" + var);
        }
    }

    @Override
    public Map<String, String> getUnifiers(Atomic parentAtom) {
        if (!(parentAtom instanceof Binary))
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());

        Map<String, String> unifiers = new HashMap<>();
        String childVarName = this.getVarName();
        String parentVarName = parentAtom.getVarName();
        String childValVarName = this.getValueVariable();
        String parentValVarName = ((Binary) parentAtom).getValueVariable();

        if (!childVarName.equals(parentVarName)) unifiers.put(childVarName, parentVarName);
        if (!childValVarName.equals(parentValVarName)) unifiers.put(childValVarName, parentValVarName);
        return unifiers;
    }
}
