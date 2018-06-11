package generator;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.*;
import storage.ConceptPicker;
import storage.ConceptStore;
import strategy.RelationshipRoleStrategy;
import strategy.RelationshipStrategy;
import strategy.RolePlayerTypeStrategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.pattern.Patterns.var;

public class RelationshipGenerator extends Generator<RelationshipStrategy> {

    private final ConceptStore conceptStore;
//    private final ConceptPicker conceptPicker;

//    private VarPattern matchVarPattern;
//    private VarPattern insertVarPattern;

    public RelationshipGenerator(RelationshipStrategy strategy, GraknTx tx, ConceptStore conceptStore,
                                 ConceptPicker conceptPicker) {
        super(strategy, tx);
        this.conceptStore = conceptStore;
//        this.conceptPicker = conceptPicker;
    }

    @Override
    public Stream<Query> generate() {
        QueryBuilder qb = this.tx.graql();

        String typeLabel = this.strategy.getTypeLabel();
        int numInstances = this.strategy.getNumInstancesPDF().next();

        Stream<Query> stream = Stream.generate(() -> {

//            Query query = qb.insert(var("x").isa(typeLabel).rel(role, roleplayer));
//            VarPattern varPattern = var("x").isa(typeLabel).rel(role, roleplayer);
//            varPattern = varPattern.rel(role, roleplayer);

            /*
            Process:
            Find roleplayer types according to the RelationshipRoleStrategy objects
             */

            String relationshipTypeLabel = strategy.getTypeLabel();

            Pattern matchVarPattern = null;
            VarPattern insertVarPattern = var("r").isa(relationshipTypeLabel);

            Set<RelationshipRoleStrategy> roleStrategies = this.strategy.getRoleStrategies();

            // For each role in the relationship
            for (RelationshipRoleStrategy roleStrategy : roleStrategies) {

                // Get the role name
                String roleLabel = roleStrategy.getRoleLabel();

                // Get the strategies for the set of types that can play that role
                Set<RolePlayerTypeStrategy> rolePlayerTypeStrategies = roleStrategy.getRolePlayerTypeStrategies();

                // For each role type strategy
                for (RolePlayerTypeStrategy rolePlayerTypeStrategy : rolePlayerTypeStrategies) {

                    // Get the name of the type that can play this role
                    String roleTypeLabel = rolePlayerTypeStrategy.getTypeLabel();
//                    if (rolePlayerTypeStrategy.getCentral()) {
//                        // In this case only find new role-players if none have yet been found
//                        // Do this by picking from the list of concept ids that have been stored by type
//
//                    } else {
                    // Find random role-players matching this type
                    // Pick ids from the list of concept ids
                    Stream<String> conceptIdStream = rolePlayerTypeStrategy.getConceptPicker().get(roleTypeLabel, conceptStore,
                            rolePlayerTypeStrategy.getNumInstancesPDF().next());

                    Iterator<String> iter = conceptIdStream.iterator();

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
            }
            Query q = qb.match(matchVarPattern).insert(insertVarPattern);
            return q;

        }).limit(numInstances);
        return stream;
    }
}
