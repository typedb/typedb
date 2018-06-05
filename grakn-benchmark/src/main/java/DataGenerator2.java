import ai.grakn.*;
import ai.grakn.concept.*;
import ai.grakn.graql.Match;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import generator.GeneratorFactory;
import pdf.DiscreteGaussianPDF;
import strategy.*;

import java.util.*;

import static ai.grakn.graql.internal.pattern.Patterns.var;

public class DataGenerator2 {

    private static String uri = "localhost:48555";
    private static String keyspace = "societal_model";

    public static final int RANDOM_SEED = 1;

    private Random rand;
    private ArrayList<EntityType> entityTypes;
    private ArrayList<RelationshipType> relationshipTypes;
    private ArrayList<AttributeType> attributeTypes;
    private ArrayList<Role> roles;

//    private Set<EntityStrategy> entityStrategies = new HashSet<EntityStrategy>();
//    private Set<RelationshipStrategy> relationshipStrategies = new HashSet<RelationshipStrategy>();
//    private Set<OperationStrategy>operationStrategies = new HashSet<OperationStrategy>();
//    private SchemaStrategy schemaStrategy;

    private FrequencyOptionCollection<EntityStrategy> entityStrategies;
    private FrequencyOptionCollection<RelationshipStrategy> relationshipStrategies;
    private FrequencyOptionCollection<AttributeStrategy> attributeStrategies;

    private FrequencyOptionCollection<FrequencyOptionCollection> operationStrategies;

    private final SchemaStrategy schemaStrategy;

    public DataGenerator2() {

        this.rand = new Random(RANDOM_SEED);
        entityStrategies = new FrequencyOptionCollection<EntityStrategy>(this.rand);
        relationshipStrategies = new FrequencyOptionCollection<RelationshipStrategy>(this.rand);
        attributeStrategies = new FrequencyOptionCollection<AttributeStrategy>(this.rand);
        operationStrategies = new FrequencyOptionCollection<FrequencyOptionCollection>(this.rand);

        GraknSession session = this.getSession();

        try (GraknTx tx = session.open(GraknTxType.READ)) {
            this.entityTypes = this.getTypes(tx, "entity");
            this.relationshipTypes = this.getTypes(tx, "relationship");
            this.attributeTypes = this.getTypes(tx, "attribute");
            this.roles = this.getTypes(tx, "role");
        }

        this.entityStrategies.add(
                0.8,
                new EntityStrategy(
                        <EntityType> this.getTypeFromString("person", this.entityTypes),
                        new DiscreteGaussianPDF(this.rand, 10.0, 2.0)));

        this.entityStrategies.add(
                0.2,
                new EntityStrategy(
                        <EntityType> this.getTypeFromString("company", this.entityTypes),
                        new DiscreteGaussianPDF(this.rand, 2.0, 1.0)));

        Set<RelationshipRoleStrategy> employmentRoleStrategies = new HashSet<RelationshipRoleStrategy>();

        employmentRoleStrategies.add(
                new RelationshipRoleStrategy(
                        <Role> this.getTypeFromString("employee", this.roles),
                        new RoleTypeStrategy(
                                <EntityType> this.getTypeFromString("person", this.entityTypes),
                                new DiscreteGaussianPDF(this.rand, 20.0, 2.0))
                )
        );

         employmentRoleStrategies.add(
                new RelationshipRoleStrategy(
                        <Role> this.getTypeFromString("employer", this.roles),
                        new RoleTypeStrategy(
                                <EntityType> this.getTypeFromString("company", this.entityTypes),
                                new DiscreteGaussianPDF(this.rand, 2.0, 1.0))
                )
        );

        this.relationshipStrategies.add(
                0.3,
                new RelationshipStrategy(
                        <RelationshipType> this.getTypeFromString("employment", this.relationshipTypes),
                        new DiscreteGaussianPDF(this.rand, 2.0, 1.0),
                        employmentRoleStrategies)
        );

        this.operationStrategies.add(1.0, this.entityStrategies);
        this.operationStrategies.add(1.0, this.relationshipStrategies);
        this.operationStrategies.add(1.0, this.attributeStrategies);

        this.schemaStrategy = new SchemaStrategy(this.operationStrategies);
    }

    private GraknSession getSession(){
        return RemoteGrakn.session(new SimpleURI(uri), Keyspace.of(keyspace));
    }

    private <T extends SchemaConcept> T getTypeFromString(String typeName, ArrayList<T> typeInstances) {
        Iterator iter = typeInstances.iterator();
        String l;
        T currentType;

        while (iter.hasNext()) {
            currentType = (T) iter.next();
            l = currentType.getLabel().toString();
            if (l.equals(typeName)){
                return currentType;
            }
        }
        return null;
    }

    private <T extends SchemaConcept> ArrayList<T> getTypes(GraknTx tx, String conceptTypeName) {
        ArrayList<T> conceptTypes = new ArrayList<T>();
        QueryBuilder qb = tx.graql();
        Match match = qb.match(var("x").sub(conceptTypeName));
        List<Answer> result = match.get().execute();
        T conceptType;

        // TODO This instead?
        // this.conceptTypes.add(result.iterator().forEachRemaining(get("x").asEntityType()));
        Iterator<Answer> conceptTypeIterator = result.iterator();
        while (conceptTypeIterator.hasNext()) {
//            conceptType = (T) conceptTypeIterator.next().get("x").asType();
            conceptType = (T) conceptTypeIterator.next().get("x");
            conceptTypes.add(conceptType);
        }
        conceptTypes.remove(0);  // Remove type "entity"

        return conceptTypes;
    }

    public void generate() {
        int max_iterations = 10;
        int it = 0;

        GraknSession session = this.getSession();

        try (GraknTx tx = session.open(GraknTxType.WRITE)) {
            while (it < max_iterations) {

//                TypeStrategy typeStrategy = this.schemaStrategy.getStrategy();
//                GeneratorFactory
//                        .create(typeStrategy, tx)
//                        .generate()
//                        .forEach(Query::execute);
                GeneratorFactory
//                        .create(this.schemaStrategy.getStrategy(), tx)
                        .create(this.operationStrategies.next().next(), tx)
                        .generate()
                        .forEach(Query::execute);

                it++;
            }
        }
    }

    public static void main(String[] args) {
        System.out.print("hello grakn\n");
        DataGenerator dg = new DataGenerator();
        System.out.print("Generating data...\n");
        dg.generate();
        System.out.print("Done\n");
    }
}
