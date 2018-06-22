/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Role;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.admin.Answer;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;
import generator.GeneratorFactory;
import generator.GeneratorInterface;
import pdf.ConstantPDF;
import pdf.DiscreteGaussianPDF;
import pdf.UniformPDF;

import pick.IsaTypeConceptIdPicker;
import pick.StreamProvider;
import pick.PickableCollectionValuePicker;
import pick.ConceptIdPicker;
import pick.CentralStreamProvider;
import pick.NotInRelationshipConceptIdStream;

import storage.ConceptTypeCountStore;
import storage.InsertionAnalysis;
import storage.SchemaManager;
import strategy.EntityStrategy;
import strategy.RelationshipStrategy;
import strategy.AttributeStrategy;
import strategy.RouletteWheelCollection;
import strategy.RolePlayerTypeStrategy;
import strategy.TypeStrategyInterface;
import strategy.AttributeOwnerTypeStrategy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;
import java.util.List;

import static ai.grakn.graql.Graql.var;

/**
 *
 */
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


    private RouletteWheelCollection<TypeStrategyInterface> entityStrategies;
    private RouletteWheelCollection<TypeStrategyInterface> relationshipStrategies;
    private RouletteWheelCollection<TypeStrategyInterface> attributeStrategies;

    private RouletteWheelCollection<RouletteWheelCollection<TypeStrategyInterface>> operationStrategies;

    private ConceptTypeCountStore conceptTypeCountStore;

    public DataGenerator() {

        this.rand = new Random(RANDOM_SEED);
        this.conceptTypeCountStore = new ConceptTypeCountStore();
        entityStrategies = new RouletteWheelCollection<>(this.rand);
        relationshipStrategies = new RouletteWheelCollection<>(this.rand);
        attributeStrategies = new RouletteWheelCollection<>(this.rand);
        operationStrategies = new RouletteWheelCollection<>(this.rand);

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
                            new UniformPDF(this.rand, 20, 40)
                    ));

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
                            new StreamProvider<>(
                                    new IsaTypeConceptIdPicker(
                                            this.rand,
                                            this.conceptTypeCountStore,
                                            "person"
                                    )
                            )
                    )
            );

            employmentRoleStrategies.add(
                    new RolePlayerTypeStrategy(
                            SchemaManager.getTypeFromString("employer", this.roles),
                            SchemaManager.getTypeFromString("company", this.entityTypes),
                            new ConstantPDF(1),
                            new CentralStreamProvider<>(
                                    new NotInRelationshipConceptIdStream(
                                            "employment",
                                            "employer",
                                            100,
//                                            new IsaTypeConceptIdPicker(
//                                                    this.rand,
//                                                    this.conceptTypeCountStore,
//                                                    "company"
//                                            )
                                            new ConceptIdPicker(
                                                    this.rand,
                                                    var("x").isa("company"),
                                                    var("x")
                                            )

                                    )
                            )
                    )
            );

            this.relationshipStrategies.add(
                    0.3,
                    new RelationshipStrategy(
                            SchemaManager.getTypeFromString("employment", this.relationshipTypes),
                            new DiscreteGaussianPDF(this.rand, 30.0, 30.0),
                            employmentRoleStrategies)
            );

            RouletteWheelCollection<String> nameValueOptions = new RouletteWheelCollection<String>(this.rand)
            .add(0.5, "Da Vinci")
            .add(0.5, "Nero");

//            TODO How to get the datatype without having to declare it? Does it make sense to do this?
//            SchemaManager.getDatatype("company", this.entityTypes),

            this.attributeStrategies.add(
                    1.0,
                    new AttributeStrategy<String>(
                            SchemaManager.getTypeFromString("name", this.attributeTypes),
                            new UniformPDF(this.rand, 3, 20),
                            new AttributeOwnerTypeStrategy(
                                    SchemaManager.getTypeFromString("company", this.entityTypes),
                                    new StreamProvider<ConceptId>(
//                                            new IsaTypeConceptIdPicker(
//                                                    this.rand,
//                                                    this.conceptTypeCountStore,
//                                                    "company")
                                            new ConceptIdPicker(
                                                    this.rand,
                                                    var("x").isa("company"),
                                                    var("x")
                                                    )
                                    )
                            ),
                            new StreamProvider<>(
                                    new PickableCollectionValuePicker<String>(nameValueOptions)
                            )
                    )
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
                TypeStrategyInterface typeStrategy = this.operationStrategies.next().next();
                System.out.print("Generating instances of concept type \"" + typeStrategy.getTypeLabel() + "\"\n");

                GeneratorInterface generator = gf.create(typeStrategy, tx); // TODO Can we do without creating a new generator each iteration

                System.out.println("Using generator " + generator.getClass().toString());
                Stream<Query> queryStream = generator.generate();
                
                this.processQueryStream(queryStream);

                iteration++;
                conceptTotal = this.conceptTypeCountStore.total();
                System.out.printf(String.format("---- %d concepts ----\n", conceptTotal), this.iteration);
                tx.commit();
            }
        }
    }

    private void processQueryStream(Stream<Query> queryStream) {
        /*
        Make the data insertions from the stream of queries generated
         */
        queryStream.map(q -> (InsertQuery) q)
                .forEach(q -> {
                    List<Answer> insertions = q.execute();
                    insertions.forEach(insert -> {
                        HashSet<Concept> insertedConcepts = InsertionAnalysis.getInsertedConcepts(q, insertions);
                        insertedConcepts.forEach(concept -> {
                            this.conceptTypeCountStore.add(concept);
                        });
                    });
                });
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
        dg.generate(100);
        dg.generate(200);
        dg.generate(300);
        dg.generate(400);
//        dg.generate(1000);
//        dg.generate(10000);
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
