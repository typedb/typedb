package grakn.core.server.kb.concept;

import grakn.core.concept.Label;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.server.session.cache.TransactionCache;

public class ConceptObserver {

    private TransactionCache transactionCache;

    public ConceptObserver(TransactionCache transactionCache) {
        this.transactionCache = transactionCache;
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
}
