package generator;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.*;
import strategy.RelationshipStrategy;
import strategy.RolePlayerTypeStrategy;

import java.util.Iterator;
import java.util.Objects;
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
            rolePlayerTypeStrategy.getPicker().reset();
        }

        String relationshipTypeLabel = strategy.getTypeLabel();

        return Stream.generate(() -> {
            /*
            Process:
            Find roleplayer types according to the RelationshipRoleStrategy objects
            Get a stream of conceptIds that can play that role, according to the picking strategy. This stream may be
            empty for one role, in which case, a decision has to be made whether to make the relationship anyway or abort
             */

            Pattern matchVarPattern = null;  //TODO It will be faster to use a pure insert, supplying the ids for the roleplayers' variables
            VarPattern insertVarPattern = var("r").isa(relationshipTypeLabel);

            boolean foundAnyRoleplayers = false;

            // For each role type strategy
            for (RolePlayerTypeStrategy rolePlayerTypeStrategy : rolePlayerTypeStrategies) {
                String roleLabel = rolePlayerTypeStrategy.getRoleLabel();

                // Find random role-players matching this type
                // Pick ids from the list of concept ids
                Stream<ConceptId> conceptIdStream = rolePlayerTypeStrategy.getPicker().getStream(rolePlayerTypeStrategy.getNumInstancesPDF(), tx);

                Iterator<ConceptId> iter = conceptIdStream.iterator();

                // Build the match insert query
                while (iter.hasNext()) {
                    foundAnyRoleplayers = true;
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

            if (foundAnyRoleplayers) {
                // Assemble the query
                return (Query) qb.match(matchVarPattern).insert(insertVarPattern);
            } else {
                System.out.println("Couldn't find any existing roleplayers for any roles in \"" + relationshipTypeLabel + "\" relationship.");
                return null;
            }

        }).limit(numInstances).filter(Objects::nonNull);
    }
}
