package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.test.rule.SampleKBContext;
import java.util.function.Consumer;

public class SimpleRuleChainKB extends TestKB {
    private final int N;

    private SimpleRuleChainKB(int N){
        this.N = N;
    }

    public static SampleKBContext context(int N) {
        return new SimpleRuleChainKB(N).makeContext();
    }

    @Override
    public Consumer<GraknTx> build(){
        return (GraknTx graph) -> {
            buildDB(graph, N);
        };
    }

    private void buildDB(GraknTx tx, int N) {
        Role fromRole = tx.putRole("fromRole");
        Role toRole = tx.putRole("toRole");

        RelationshipType relation0 = tx.putRelationshipType("relation0")
                .relates(fromRole)
                .relates(toRole);

        for (int i = 1; i <= N; i++) {
            tx.putRelationshipType("relation" + i)
                    .relates(fromRole)
                    .relates(toRole);
        }
        EntityType genericEntity = tx.putEntityType("genericEntity")
                .plays(fromRole)
                .plays(toRole);

        Entity fromEntity = genericEntity.addEntity();
        Entity toEntity = genericEntity.addEntity();

        relation0.addRelationship()
                .addRolePlayer(fromRole, fromEntity)
                .addRolePlayer(toRole, toEntity);

        for (int i = 1; i <= N; i++) {
            Var fromVar = Graql.var().asUserDefined();
            Var toVar = Graql.var().asUserDefined();
            VarPattern rulePattern = Graql
                    .label("rule" + i)
                    .sub("rule")
                    .when(
                            Graql.and(
                                    Graql.var()
                                            .rel(Graql.label(fromRole.getLabel()), fromVar)
                                            .rel(Graql.label(toRole.getLabel()), toVar)
                                            .isa("relation" + (i - 1))
                            )
                    )
                    .then(
                            Graql.and(
                                    Graql.var()
                                            .rel(Graql.label(fromRole.getLabel()), fromVar)
                                            .rel(Graql.label(toRole.getLabel()), toVar)
                                            .isa("relation" + i)
                            )
                    );
            tx.graql().define(rulePattern).execute();
        }
    }
}
