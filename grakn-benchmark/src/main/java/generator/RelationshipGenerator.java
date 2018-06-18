package generator;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.*;
import strategy.RelationshipStrategy;
import strategy.RolePlayerTypeStrategy;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.pattern.Patterns.var;

public class RelationshipGenerator extends Generator<RelationshipStrategy> {

    public RelationshipGenerator(RelationshipStrategy strategy, GraknTx tx) {
        super(strategy, tx);
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

            Pattern matchVarPattern = null;  //TODO It will be faster to use a pure insert, supplying the ids for the roleplayers' variables
            VarPattern insertVarPattern = var("r").isa(relationshipTypeLabel);

                // For each role type strategy
                for (RolePlayerTypeStrategy rolePlayerTypeStrategy : rolePlayerTypeStrategies) {
                    String roleLabel = rolePlayerTypeStrategy.getRoleLabel();

                    // Get the name of the type that can play this role
                    String roleTypeLabel = rolePlayerTypeStrategy.getTypeLabel();
                    // Find random role-players matching this type
                    // Pick ids from the list of concept ids

                    Stream<ConceptId> conceptIdStream = rolePlayerTypeStrategy.getConceptPicker().getConceptIdStream(rolePlayerTypeStrategy.getNumInstancesPDF(), tx);

                    Iterator<ConceptId> iter = conceptIdStream.iterator();

                    // Build the match insert query
                    while (iter.hasNext()) {
                        ConceptId conceptId = iter.next();
                        // Add the concept to the query
                        Var v = Graql.var().asUserDefined();
                        if (matchVarPattern == null) {
                            matchVarPattern = v.id(conceptId);
                        } else {
                            Pattern varPattern = v.id(conceptId);
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
