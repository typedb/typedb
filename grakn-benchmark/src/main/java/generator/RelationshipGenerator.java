package generator;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Role;
import ai.grakn.graql.*;
import storage.ConceptPicker;
import storage.ConceptStore;
import strategy.RelationshipStrategy;
import strategy.RolePlayerTypeStrategy;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.pattern.Patterns.var;

public class RelationshipGenerator extends Generator<RelationshipStrategy> {

//    private final ConceptStore conceptStore;

//    public RelationshipGenerator(RelationshipStrategy strategy, GraknTx tx, ConceptStore conceptStore) {
    public RelationshipGenerator(RelationshipStrategy strategy, GraknTx tx) {
        super(strategy, tx);
//        this.conceptStore = conceptStore;
    }

    @Override
    public Stream<Query> generate() {
        QueryBuilder qb = this.tx.graql();

        int numInstances = this.strategy.getNumInstancesPDF().next();

        Set<RolePlayerTypeStrategy> rolePlayerTypeStrategies = this.strategy.getRolePlayerTypeStrategies();
        for (RolePlayerTypeStrategy rolePlayerTypeStrategy : rolePlayerTypeStrategies) {
            // Reset the roleplayer pickers to cater for the case where they are central
            rolePlayerTypeStrategy.getConceptPicker().reset();
        }

        Stream<Query> stream = Stream.generate(() -> {

            /*
            Process:
            Find roleplayer types according to the RelationshipRoleStrategy objects
             */

            String relationshipTypeLabel = strategy.getTypeLabel();

            Pattern matchVarPattern = null;
            VarPattern insertVarPattern = var("r").isa(relationshipTypeLabel);

                // For each role type strategy
                for (RolePlayerTypeStrategy rolePlayerTypeStrategy : rolePlayerTypeStrategies) {
                    String roleLabel = rolePlayerTypeStrategy.getRoleLabel();

                    // Get the name of the type that can play this role
                    String roleTypeLabel = rolePlayerTypeStrategy.getTypeLabel();
                    // Find random role-players matching this type
                    // Pick ids from the list of concept ids


//                    Stream<String> conceptIdStream = rolePlayerTypeStrategy.getConceptPicker().get(roleTypeLabel, conceptStore,
//                            rolePlayerTypeStrategy.getNumInstancesPDF().next());

                    Stream<String> conceptIdStream = rolePlayerTypeStrategy.getConceptPicker().get(rolePlayerTypeStrategy.getNumInstancesPDF(), tx);

                    Iterator<String> iter = conceptIdStream.iterator();

                    // Build the match insert query
                    while (iter.hasNext()) {
                        String conceptId = iter.next();
                        // Add the concept to the query
                        Var v = Graql.var().asUserDefined();
                        if (matchVarPattern == null) {
                            matchVarPattern = v.id(ConceptId.of(conceptId));
                        } else {
                            Pattern varPattern = v.id(ConceptId.of(conceptId));
                            matchVarPattern = matchVarPattern.and(varPattern);
                        }

                        insertVarPattern = insertVarPattern.rel(roleLabel, v);
                    }
                }

            // Assemble the query
            Query q = qb.match(matchVarPattern).insert(insertVarPattern);
            return q;

        }).limit(numInstances);
        return stream;
    }
}
