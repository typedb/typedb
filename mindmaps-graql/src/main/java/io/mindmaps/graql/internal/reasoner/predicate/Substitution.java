package io.mindmaps.graql.internal.reasoner.predicate;


import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.model.Concept;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.reasoner.container.Query;

import java.util.Set;

public class Substitution extends AtomBase{

    private final String val;

    public Substitution(VarAdmin pattern)
    {
        super(pattern);
        this.val = extractValue(pattern);
    }

    public Substitution(VarAdmin pattern, Query par)
    {
        super(pattern, par);
        this.val = extractValue(pattern);
    }

    public Substitution(Substitution a)
    {
        super(a);
        this.val = extractValue(a.getPattern().asVar());
    }

    public Substitution(String name, Concept con)
    {
        super(Graql.var(name).id(con.getId()).admin().asVar());
        this.val = con.getId();
    }

    @Override
    public boolean isValuePredicate(){ return true;}

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof Substitution)) return false;
        Substitution a2 = (Substitution) obj;
        return this.getVarName().equals(a2.getVarName()) && this.getVal().equals(a2.getVal());
    }

    @Override
    public boolean isEquivalent(Object obj){
        if (!(obj instanceof Substitution)) return false;
        Substitution a2 = (Substitution) obj;
        return this.getVal().equals(a2.getVal());
    }

    @Override
    public int hashCode()
    {
        int hashCode = 1;
        hashCode = hashCode * 37 + this.val.hashCode();
        hashCode = hashCode * 37 + this.varName.hashCode();
        return hashCode;
    }

    @Override
    public String getVal(){ return val;}

    private String extractValue(VarAdmin var) {

        String value = "";

        if (!var.admin().getValuePredicates().isEmpty()){
            Set<ValuePredicateAdmin> valuePredicates = var.admin().getValuePredicates();
            if (valuePredicates.size() != 1)
                throw new IllegalArgumentException(ErrorMessage.MULTIPLE_VALUE_PREDICATES.getMessage(atomPattern.toString()));
            else
                value = valuePredicates.iterator().next().getPredicate().getValue().toString();
        }
        else if(var.admin().getId().isPresent()) value = var.admin().getId().get();

        return value;

    }

}
