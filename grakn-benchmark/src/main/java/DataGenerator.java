import ai.grakn.*;
import ai.grakn.concept.*;
import ai.grakn.graql.*;
import ai.grakn.graql.admin.Answer;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import generator.Generator;
import generator.GeneratorFactory;
import pdf.ConstantPDF;
import pdf.DiscreteGaussianPDF;
import pdf.UniformPDF;
import pick.*;
import storage.*;
import strategy.*;

import java.util.*;
import java.util.stream.Stream;

public class DataGenerator {

    private static String uri = "localhost:48555";
    private static String keyspace = "societal_model";
    private static String schemaRelativeDirPath = "/grakn-benchmark/src/main/resources/societal_model.gql";

    public static final int RANDOM_SEED = 1;
    private int iteration = 0;

    private Random rand;
    private ArrayList<EntityType> entityTypes;
    private ArrayList<RelationshipType> relationshipTypes;
    private ArrayList<AttributeType> attributeTypes;
    private ArrayList<Role> roles;


    private RouletteWheelCollection<EntityStrategy> entityStrategies;
    private RouletteWheelCollection<RelationshipStrategy> relationshipStrategies;
    private RouletteWheelCollection<AttributeStrategy> attributeStrategies;

    private RouletteWheelCollection<RouletteWheelCollection> operationStrategies;

    private ConceptTypeCountStore conceptTypeCountStore;

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
            this.entityTypes = SchemaManager.getTypes(tx, "entity");
            this.relationshipTypes = SchemaManager.getTypes(tx, "relationship");
            this.attributeTypes = SchemaManager.getTypes(tx, "attribute");
            this.roles = SchemaManager.getTypes(tx, "role");


            this.entityStrategies.add(
                    0.5,
                    new EntityStrategy(
                            SchemaManager.getTypeFromString("person", this.entityTypes),
//                            new DiscreteGaussianPDF(this.rand, 10.0, 2.0)
                            new UniformPDF(this.rand, 20, 40)
                    ));

//            this.entityStrategies.add(
//                    0.5,
//                    new EntityStrategy(
//                            SchemaManager.getTypeFromString("occupation", this.entityTypes),
//                            new DiscreteGaussianPDF(this.rand, 2.0, 1.0)));

            this.entityStrategies.add(
                    0.5,
                    new EntityStrategy(
                            SchemaManager.getTypeFromString("company", this.entityTypes),
                            new UniformPDF(this.rand, 1, 5)
                    )
            );

            Set<RolePlayerTypeStrategy> employmentRoleStrategies = new HashSet<RolePlayerTypeStrategy>();

            employmentRoleStrategies.add(
                    new RolePlayerTypeStrategy(
                            SchemaManager.getTypeFromString("employee", this.roles),
                            SchemaManager.getTypeFromString("person", this.entityTypes),
                            new ConstantPDF(1),
                            new ConceptIdStreamLimiter(
//                                    new NotInRelationshipConceptIdStream(
//                                            "employment",
//                                            "employee",
//                                            100,
                                    new IsaTypeConceptIdPicker(
                                            this.rand,
                                            this.conceptTypeCountStore,
                                            "person"
                                    )
//                                    )
                            )
                    )
            );

            employmentRoleStrategies.add(
                    new RolePlayerTypeStrategy(
                            SchemaManager.getTypeFromString("employer", this.roles),
                            SchemaManager.getTypeFromString("company", this.entityTypes),
                            new ConstantPDF(1),
//                            new DiscreteGaussianPDF(this.rand, 1.0, 1.0),
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

            this.relationshipStrategies.add(
                    0.3,
                    new RelationshipStrategy(
                            SchemaManager.getTypeFromString("employment", this.relationshipTypes),
//                            new UniformPDF(this.rand, 2, 30),
                            new DiscreteGaussianPDF(this.rand, 30.0, 30.0),
                            employmentRoleStrategies)
            );
        }

        this.operationStrategies.add(0.7, this.entityStrategies);
        this.operationStrategies.add(0.3, this.relationshipStrategies);
        this.operationStrategies.add(0.0, this.attributeStrategies);

    }

    private GraknSession getSession() {
        return RemoteGrakn.session(new SimpleURI(uri), Keyspace.of(keyspace));
    }

    public void generate(int numConceptsLimit) {
        /*
        This method can be called multiple times, with a higher numConceptsLimit each time, so that the generation can be
        effectively paused while benchmarking takes place
        */

        GraknSession session = this.getSession();

        GeneratorFactory gf = new GeneratorFactory();
        int conceptTotal = this.conceptTypeCountStore.total();

        while (conceptTotal < numConceptsLimit) {
            System.out.printf("---- Iteration %d ----\n", this.iteration);
            try (GraknTx tx = session.open(GraknTxType.WRITE)) {
                
                //TODO Deal with this being an Object. TypeStrategy should be/have an interface for this purpose?
                Object typeStrategy = this.operationStrategies.next().next();
                System.out.print("Generating instances of concept type \"" + ((TypeStrategy) typeStrategy).getTypeLabel() + "\"\n");

                Generator generator = gf.create(typeStrategy, tx); // TODO Can we do without creating a new generator each iteration

                System.out.println("Using generator " + generator.getClass().toString());
                Stream<Query> queriesStream = generator.generate();

                queriesStream.map(q -> (InsertQuery) q)
                        .forEach(q -> {  // TODO Remove ordering?
                            List<Answer> insertions = q.execute();
                            insertions.forEach(insert -> {
                                HashSet<Concept> insertedConcepts = InsertionAnalysis.getInsertedConcepts(q, insertions);
                                insertedConcepts.forEach(concept -> {
                                    this.conceptTypeCountStore.add(concept); //TODO Store count is being totalled wrong
                                });
                            });
                        });
                iteration++;
                conceptTotal = this.conceptTypeCountStore.total();
                System.out.printf(String.format("---- %d concepts ----\n", conceptTotal), this.iteration);
                tx.commit();
            }
        }
    }

    public void reset() {
        System.out.println("Initialising keyspace...");
        SchemaManager.initialise(this.getSession(), schemaRelativeDirPath);
        System.out.println("done");
        this.iteration = 0;
    }

    public static void main(String[] args) {
        DataGenerator dg = new DataGenerator();
        System.out.print("Generating data...\n");
        dg.reset();

        long startTime = System.nanoTime();
//        dg.generate(631);
        dg.generate(100);
        dg.generate(200);
        dg.generate(300);
        dg.generate(400);
        dg.generate(1000);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000000;

        long hours = duration / 3600;
        long minutes = (duration % 3600) / 60;
        long seconds = duration % 60;

        String timeString = String.format("%02d:%02d:%02d", hours, minutes, seconds);

        System.out.printf("Generation took %s\n", timeString);
        System.out.print("Done\n");
    }
}
