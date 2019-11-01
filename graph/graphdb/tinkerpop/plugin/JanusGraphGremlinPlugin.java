// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.tinkerpop.plugin;

import grakn.core.graph.core.BaseVertexQuery;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.ConfiguredGraphFactory;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.Idfiable;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.JanusGraphComputer;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphFactory;
import grakn.core.graph.core.JanusGraphIndexQuery;
import grakn.core.graph.core.JanusGraphMultiVertexQuery;
import grakn.core.graph.core.JanusGraphProperty;
import grakn.core.graph.core.JanusGraphQuery;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.JanusGraphVertexQuery;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.Namifiable;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.QueryDescription;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.TransactionBuilder;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.core.VertexList;
import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.core.attribute.Cmp;
import grakn.core.graph.core.attribute.Contain;
import grakn.core.graph.core.attribute.Geo;
import grakn.core.graph.core.attribute.Geoshape;
import grakn.core.graph.core.attribute.JtsGeoshapeHelper;
import grakn.core.graph.core.attribute.Text;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.core.schema.DefaultSchemaMaker;
import grakn.core.graph.core.schema.EdgeLabelMaker;
import grakn.core.graph.core.schema.Index;
import grakn.core.graph.core.schema.JanusGraphConfiguration;
import grakn.core.graph.core.schema.JanusGraphIndex;
import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.core.schema.JanusGraphSchemaElement;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.core.schema.JobStatus;
import grakn.core.graph.core.schema.Mapping;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.core.schema.PropertyKeyMaker;
import grakn.core.graph.core.schema.RelationTypeIndex;
import grakn.core.graph.core.schema.RelationTypeMaker;
import grakn.core.graph.core.schema.SchemaAction;
import grakn.core.graph.core.schema.SchemaInspector;
import grakn.core.graph.core.schema.SchemaManager;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.core.schema.VertexLabelMaker;
import grakn.core.graph.graphdb.database.management.ManagementSystem;
import grakn.core.graph.graphdb.management.ConfigurationManagementGraph;
import grakn.core.graph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

public class JanusGraphGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "janusgraph.imports";

    private final static Set<Class> CLASS_IMPORTS = new LinkedHashSet<>();
    private final static Set<Enum> ENUM_IMPORTS = new LinkedHashSet<>();
    private final static Set<Method> METHOD_IMPORTS = new LinkedHashSet<>();

    static {
        /////////////
        // CLASSES //
        /////////////

        CLASS_IMPORTS.add(BaseVertexQuery.class);
        CLASS_IMPORTS.add(Cardinality.class);
        CLASS_IMPORTS.add(ConfiguredGraphFactory.class);
        CLASS_IMPORTS.add(EdgeLabel.class);
        CLASS_IMPORTS.add(Idfiable.class);
        CLASS_IMPORTS.add(JanusGraph.class);
        CLASS_IMPORTS.add(JanusGraphComputer.class);
        CLASS_IMPORTS.add(JanusGraphEdge.class);
        CLASS_IMPORTS.add(JanusGraphElement.class);
        CLASS_IMPORTS.add(JanusGraphFactory.class);
        CLASS_IMPORTS.add(JanusGraphIndexQuery.class);
        CLASS_IMPORTS.add(JanusGraphMultiVertexQuery.class);
        CLASS_IMPORTS.add(JanusGraphProperty.class);
        CLASS_IMPORTS.add(JanusGraphQuery.class);
        CLASS_IMPORTS.add(JanusGraphRelation.class);
        CLASS_IMPORTS.add(JanusGraphTransaction.class);
        CLASS_IMPORTS.add(JanusGraphVertex.class);
        CLASS_IMPORTS.add(JanusGraphVertexProperty.class);
        CLASS_IMPORTS.add(JanusGraphVertexQuery.class);
        CLASS_IMPORTS.add(Multiplicity.class);
        CLASS_IMPORTS.add(Namifiable.class);
        CLASS_IMPORTS.add(PropertyKey.class);
        CLASS_IMPORTS.add(QueryDescription.class);
        CLASS_IMPORTS.add(RelationType.class);
        CLASS_IMPORTS.add(JanusGraphTransaction.class);
        CLASS_IMPORTS.add(TransactionBuilder.class);
        CLASS_IMPORTS.add(VertexLabel.class);
        CLASS_IMPORTS.add(VertexList.class);

        CLASS_IMPORTS.add(AttributeSerializer.class);
        CLASS_IMPORTS.add(Cmp.class);
        CLASS_IMPORTS.add(Contain.class);
        CLASS_IMPORTS.add(Geo.class);
        CLASS_IMPORTS.add(Geoshape.class);
        CLASS_IMPORTS.add(JtsGeoshapeHelper.class);
        CLASS_IMPORTS.add(Text.class);

        CLASS_IMPORTS.add(ConsistencyModifier.class);
        CLASS_IMPORTS.add(DefaultSchemaMaker.class);
        CLASS_IMPORTS.add(EdgeLabelMaker.class);
        CLASS_IMPORTS.add(Index.class);
        CLASS_IMPORTS.add(JanusGraphConfiguration.class);
        CLASS_IMPORTS.add(JanusGraphIndex.class);
        CLASS_IMPORTS.add(JanusGraphManagement.class);
        CLASS_IMPORTS.add(JanusGraphSchemaElement.class);
        CLASS_IMPORTS.add(JanusGraphSchemaType.class);
        CLASS_IMPORTS.add(JobStatus.class);
        CLASS_IMPORTS.add(Mapping.class);
        CLASS_IMPORTS.add(Parameter.class);
        CLASS_IMPORTS.add(PropertyKeyMaker.class);
        CLASS_IMPORTS.add(RelationTypeIndex.class);
        CLASS_IMPORTS.add(RelationTypeMaker.class);
        CLASS_IMPORTS.add(SchemaAction.class);
        CLASS_IMPORTS.add(SchemaInspector.class);
        CLASS_IMPORTS.add(SchemaManager.class);
        CLASS_IMPORTS.add(SchemaStatus.class);
        CLASS_IMPORTS.add(VertexLabelMaker.class);

        CLASS_IMPORTS.add(JanusGraphIoRegistry.class);
        CLASS_IMPORTS.add(ConfigurationManagementGraph.class);
        CLASS_IMPORTS.add(ManagementSystem.class);

        CLASS_IMPORTS.add(Instant.class);
        CLASS_IMPORTS.add(Clock.class);
        CLASS_IMPORTS.add(DayOfWeek.class);
        CLASS_IMPORTS.add(Duration.class);
        CLASS_IMPORTS.add(LocalDate.class);
        CLASS_IMPORTS.add(LocalTime.class);
        CLASS_IMPORTS.add(LocalDateTime.class);
        CLASS_IMPORTS.add(Month.class);
        CLASS_IMPORTS.add(MonthDay.class);
        CLASS_IMPORTS.add(OffsetDateTime.class);
        CLASS_IMPORTS.add(OffsetTime.class);
        CLASS_IMPORTS.add(Period.class);
        CLASS_IMPORTS.add(Year.class);
        CLASS_IMPORTS.add(YearMonth.class);
        CLASS_IMPORTS.add(ZonedDateTime.class);
        CLASS_IMPORTS.add(ZoneId.class);
        CLASS_IMPORTS.add(ZoneOffset.class);
        CLASS_IMPORTS.add(ChronoUnit.class);

        ///////////
        // ENUMS //
        ///////////

        // also make sure the class is imported for these enums

        Collections.addAll(ENUM_IMPORTS, Multiplicity.values());
        Collections.addAll(ENUM_IMPORTS, Cardinality.values());
        Collections.addAll(ENUM_IMPORTS, ChronoUnit.values());

        /////////////
        // METHODS //
        /////////////

        // only include the static predicates
        // also make sure the class is imported for these methods

        Stream.of(Geo.values())
                .map(v -> {
                    try {
                        return Geo.class.getMethod(v.toString(), Object.class);
                    } catch (NoSuchMethodException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(JanusGraphGremlinPlugin::isMethodStatic)
                .forEach(METHOD_IMPORTS::add);

        Stream.of(Text.values())
                .map(v -> {
                    try {
                        return Text.class.getMethod(v.toString(), Object.class);
                    } catch (NoSuchMethodException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(JanusGraphGremlinPlugin::isMethodStatic)
                .forEach(METHOD_IMPORTS::add);
    }

    private static final ImportCustomizer IMPORTS = DefaultImportCustomizer.build()
            .addClassImports(CLASS_IMPORTS)
            .addEnumImports(ENUM_IMPORTS)
            .addMethodImports(METHOD_IMPORTS)
            .create();

    private static final JanusGraphGremlinPlugin instance = new JanusGraphGremlinPlugin();

    public JanusGraphGremlinPlugin() {
        super(NAME, IMPORTS);
    }

    public static JanusGraphGremlinPlugin instance() {
        return instance;
    }

    public JanusGraphGremlinPlugin(String moduleName, Customizer... customizers) {
        super(moduleName, customizers);
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    private static boolean isMethodStatic(Method method) {
        return Modifier.isStatic(method.getModifiers());
    }

}
