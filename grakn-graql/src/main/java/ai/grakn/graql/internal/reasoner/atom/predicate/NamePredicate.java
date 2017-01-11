package ai.grakn.graql.internal.reasoner.atom.predicate;

import ai.grakn.concept.Concept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.NameProperty;
import ai.grakn.graql.internal.reasoner.query.Query;


public class NamePredicate extends Predicate<String>{

    public NamePredicate(VarAdmin pattern) { super(pattern);}
    public NamePredicate(VarName varName, NameProperty prop){
        this(createNameVar(varName, prop.getNameValue()));
    }
    private NamePredicate(NamePredicate a) { super(a);}

    public static VarAdmin createNameVar(VarName varName, String typeName){
        return Graql.var(varName).name(typeName).admin();
    }

    @Override
    public Atomic clone(){
        return new NamePredicate(this);
    }

    @Override
    public String getPredicateValue() { return predicate;}

    @Override
    protected String extractPredicate(VarAdmin var){ return var.admin().getTypeName().orElse("");}
}
