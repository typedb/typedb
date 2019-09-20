package grakn.core.server.kb.concept;

import grakn.core.concept.Label;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.server.session.cache.RuleCache;
import grakn.core.server.session.cache.TransactionCache;
import grakn.core.server.statistics.UncomittedStatisticsDelta;

public class ConceptObserverReference {

    private TransactionCache transactionCache;
    private UncomittedStatisticsDelta uncomittedStatisticsDelta;
    private RuleCache ruleCache;
    private MultilevelSemanticCache queryCache;

    public ConceptObserverReference() {
    }

    public void deleteThing(Thing thing) {

    }

    public void deleteSchemaConcept(SchemaConcept schemaConcept) {

    }

    public void createThing(Thing thing) {

    }

    public void createAttribute(Thing thing, Label label, String index) {

    }

    public void createSchemaConcept(SchemaConcept schemaConcept) {

    }

    public void conceptSetAbstract(boolean isAbstract) {

    }

    public void createHasAttribute(boolean isAbstract) {

    }

    public void setTransactionCache(TransactionCache transactionCache) {
        this.transactionCache = transactionCache;
    }

    public void setQueryCache(MultilevelSemanticCache queryCache) {
        this.queryCache = queryCache;
    }

    public void setRuleCache(RuleCache ruleCache) {
        this.ruleCache = ruleCache;
    }

    public void setStatisticsDelta(UncomittedStatisticsDelta uncomittedStatisticsDelta) {
        this.uncomittedStatisticsDelta = uncomittedStatisticsDelta;
    }
}
