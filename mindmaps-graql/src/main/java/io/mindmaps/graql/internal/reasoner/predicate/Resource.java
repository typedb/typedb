package io.mindmaps.graql.internal.reasoner.predicate;

import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.reasoner.query.Query;
import io.mindmaps.util.ErrorMessage;
import java.util.Map;
import java.util.Set;

public class Resource extends AtomBase{

    private final String val;

    public Resource(VarAdmin pattern) {
        super(pattern);
        this.val = extractValue(pattern);
    }

    public Resource(VarAdmin pattern, Query par) {
        super(pattern, par);
        this.val = extractValue(pattern);
    }

    public Resource(Resource a) {
        super(a);
        this.val = extractValue(a.getPattern().asVar());
    }

    @Override
    public Atomic clone(){
        return new Resource(this);
    }

    @Override
    public boolean isUnary(){ return true;}

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Resource)) return false;
        Resource a2 = (Resource) obj;
        return this.typeId.equals(a2.getTypeId()) && this.varName.equals(a2.getVarName())
                && this.val.equals(a2.getVal());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.val.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public boolean isEquivalent(Object obj) {
        if (!(obj instanceof Resource)) return false;
        Resource a2 = (Resource) obj;
        return this.typeId.equals(a2.getTypeId()) && this.val.equals(a2.getVal());
    }

    @Override
    public int equivalenceHashCode(){
        int hashCode = 1;
        hashCode = hashCode * 37 + this.typeId.hashCode();
        hashCode = hashCode * 37 + this.val.hashCode();
        return hashCode;
    }

    @Override
    public String getVal(){ return val;}

    private String extractValue(VarAdmin var) {
        String value = "";
        Map<VarAdmin, Set<ValuePredicateAdmin>> resourceMap = var.getResourcePredicates();
        if (resourceMap.size() != 0) {
            if (resourceMap.size() != 1)
                throw new IllegalArgumentException(ErrorMessage.PATTERN_NOT_VAR.getMessage(this.toString()));

            Map.Entry<VarAdmin, Set<ValuePredicateAdmin>> entry = resourceMap.entrySet().iterator().next();
            value = entry.getValue().iterator().hasNext()? entry.getValue().iterator().next().getPredicate().getValue().toString() : "";
        }
        return value;
    }
}
