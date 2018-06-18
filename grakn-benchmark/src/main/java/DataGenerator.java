import ai.grakn.*;
import ai.grakn.concept.*;
import ai.grakn.graql.*;
import ai.grakn.graql.admin.Answer;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import com.google.common.io.Files;
import generator.Generator;
import generator.GeneratorFactory;
import pdf.DiscreteGaussianPDF;
import pick.*;
import storage.*;
import strategy.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.pattern.Patterns.var;

public class DataGenerator {

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

    private RouletteWheelCollection<EntityStrategy> entityStrategies;
    private RouletteWheelCollection<RelationshipStrategy> relationshipStrategies;
    private RouletteWheelCollection<AttributeStrategy> attributeStrategies;

//    private RouletteWheelCollection<RouletteWheelCollection<TypeStrategy>> operationStrategies;
    private RouletteWheelCollection<RouletteWheelCollection> operationStrategies;

    private boolean doExecution = true;
    private ConceptTypeCountStore conceptTypeCountStore;
//    private RandomConceptIdPicker conceptPicker;

    public DataGenerator() {

        this.rand = new Random(RANDOM_SEED);
        this.conceptTypeCountStore = new ConceptTypeCountStore();
        entityStrategies = new RouletteWheelCollection<EntityStrategy>(this.rand);
        relationshipStrategies = new RouletteWheelCollection<RelationshipStrategy>(this.rand);
        attributeStrategies = new RouletteWheelCollection<AttributeStrategy>(this.rand);
//        operationStrategies = new RouletteWheelCollection<RouletteWheelCollection<TypeStrategy>>(this.rand);
        operationStrategies = new RouletteWheelCollection<RouletteWheelCollection>(this.rand);

        GraknSession session = this.getSession();

        try (GraknTx tx = session.open(GraknTxType.READ)) {

            // TODO Add checking to ensure that all of these strategies make sense
            this.entityTypes = this.getTypes(tx, "entity");
            this.relationshipTypes = this.getTypes(tx, "relationship");
            this.attributeTypes = this.getTypes(tx, "attribute");
            this.roles = this.getTypes(tx, "role");


//            this.entityStrategies.add(
//                    0.5,
//                    new EntityStrategy(
//                            this.getTypeFromString("person", this.entityTypes),
//                            new DiscreteGaussianPDF(this.rand, 10.0, 2.0)));

//            this.entityStrategies.add(
//                    0.5,
//                    new EntityStrategy(
//                            this.getTypeFromString("occupation", this.entityTypes),
//                            new DiscreteGaussianPDF(this.rand, 2.0, 1.0)));

            this.entityStrategies.add(
                    0.5,
                    new EntityStrategy(
                            this.getTypeFromString("company", this.entityTypes),
                            new DiscreteGaussianPDF(this.rand, 2.0, 1.0)));

            Set<RolePlayerTypeStrategy> employmentRoleStrategies = new HashSet<RolePlayerTypeStrategy>();

//            employmentRoleStrategies.add(
//                    new RolePlayerTypeStrategy(
//                            this.getTypeFromString("employee", this.roles),
//                            this.getTypeFromString("person", this.entityTypes),
//                            new DiscreteGaussianPDF(this.rand, 20.0, 10.0),
//                            new RolePlayerConceptPicker(this.rand,
//                                    "person",
//                                    "employment",
//                                    "employee",
//                                    this.conceptTypeCountStore)
//                    )
//            );

            employmentRoleStrategies.add(
                    new RolePlayerTypeStrategy(
                            this.getTypeFromString("employer", this.roles),
                            this.getTypeFromString("company", this.entityTypes),
                            new DiscreteGaussianPDF(this.rand, 1.0, 1.0),
                            new CentralConceptIdStreamLimiter(
                                    new NotInRelationshipConceptIdStream(
                                            "employment",
                                            "employer",
                                            100,
                                            new IsaTypeConceptIdPicker(
                                                    this.rand,
                                                    this.conceptTypeCountStore,
                                                    "company"
                                            )

                                    )
                            )
                    )
            );
//            employmentRoleStrategies.add(
//                    new RolePlayerTypeStrategy(
//                            this.getTypeFromString("employee", this.roles),
//                            this.getTypeFromString("person", this.entityTypes),
//                            new DiscreteGaussianPDF(this.rand, 1.0, 1.0),
//                            new ConceptIdStreamLimiter(
//                                    new IsaTypeConceptIdPicker(
//                                            this.rand,
//                                            this.conceptTypeCountStore,
//                                            "person"
//                                    )
//
//                            )
//                    )
//            );

//            employmentRoleStrategies.add(
//                    new RelationshipRoleStrategy(
//                            this.getTypeFromString("profession", this.roles),
//                            new RolePlayerTypeStrategy(
//                                    this.getTypeFromString("occupation", this.entityTypes),
//                                    new DiscreteGaussianPDF(this.rand, 2.0, 1.0),
//                                    new RandomConceptIdPicker(this.rand, false))
//                    )
//            );

            this.relationshipStrategies.add(
                    0.3,
                    new RelationshipStrategy(
                            this.getTypeFromString("employment", this.relationshipTypes),
                            new DiscreteGaussianPDF(this.rand, 2.0, 1.0),
                            employmentRoleStrategies)
            );
        }

        this.operationStrategies.add(0.8, this.entityStrategies);
        this.operationStrategies.add(0.2, this.relationshipStrategies);
        this.operationStrategies.add(0.0, this.attributeStrategies);

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
        throw new RuntimeException("Couldn't find a concept type with name \"" + typeName + "\"");
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

    private void deleteExistingConcepts() {
//        Runtime rt = Runtime.getRuntime();


        File graql = new File(System.getProperty("user.dir") + "/grakn-benchmark/src/main/resources/societal_model.gql");

        List<String> queries;
        try {
            queries = Files.readLines(graql, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        GraknSession session = this.getSession();
        try (GraknTx tx = session.open(GraknTxType.WRITE)) {

            QueryBuilder qb = tx.graql();
            Var x = Graql.var().asUserDefined();  //TODO This needed to be asUserDefined or else getting error: ai.grakn.exception.GraqlQueryException: the variable $1528883020589004 is not in the query
            Var y = Graql.var().asUserDefined();

            // qb.match(x.isa("thing")).delete(x).execute();  // TODO Only got a complaint at runtime when using delete() without supplying a variable
            // TODO Sporadically has errors, logged in bug #20200

            qb.match(x.isa("attribute")).delete(x).execute();
            qb.match(x.isa("relationship")).delete(x).execute();
            qb.match(x.isa("entity")).delete(x).execute();

            //
//            qb.undefine(y.sub("thing")).execute(); // TODO undefine $y sub thing; doesn't work/isn't supported
            // TODO undefine $y sub entity; also doesn't work, you need to be specific with undefine

            List<Answer> schema = qb.match(y.sub("thing")).get().execute();

            for (Answer element : schema) {
                Var z = Graql.var().asUserDefined();
                qb.undefine(z.id(element.get(y).getId())).execute();
            }

            tx.graql().parser().parseList(queries.stream().collect(Collectors.joining("\n"))).forEach(Query::execute);

            tx.commit();

        }
    }

    public void generate() {
        // TODO Clean and rebuild the keyspace every time to avoid unpredictable behaviour!

        this.deleteExistingConcepts();

        int max_iterations = 30;
        int it = 0;

        // Store the ids of the concepts inserted by type, using type as the key
//        this.conceptTypeCountStore = new HashMap<String, ArrayList<String>>();
//        this.conceptTypeCountStore = new ConceptTypeCountStore();
//        this.conceptPicker = new RandomConceptIdPicker(this.rand, false);

        GraknSession session = this.getSession();

            GeneratorFactory gf = new GeneratorFactory();

            while (it < max_iterations) {
                try (GraknTx tx = session.open(GraknTxType.WRITE)) {
//                Generator generator = gf.create(this.operationStrategies.next().next(), tx, this.conceptTypeCountStore, this.conceptPicker);
                Generator generator = gf.create(this.operationStrategies.next().next(), tx); // TODO Can we do without creating a new generator each iteration

                Stream<Query> queriesStream = generator.generate();

                    if (this.doExecution) {
                        queriesStream.map(q -> (InsertQuery) q)
                                .forEachOrdered(q -> {  // TODO Remove ordering?
                                    List<Answer> insertions = q.execute();
                                    insertions.forEach(insert -> {
                                        HashSet<Concept> insertedConcepts = InsertionAnalysis.getInsertedConcepts(q, insertions);
                                        insertedConcepts.forEach(concept -> {
                                            this.conceptTypeCountStore.add(concept); //TODO Store count is being totalled wrong
                                        });
                                    });
                                });
                    } else {

                        // Print the queries rather than executing
                        Iterator<Query> iter = queriesStream.iterator();

                        while (iter.hasNext()) {
                            String s = iter.next().toString();
                            System.out.print(s + "\n");
                        }
                    }
                    it++;
                    if (this.doExecution) {
                        tx.commit();
                    }
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
