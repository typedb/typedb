/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.engine.postprocessing;

import ai.grakn.Grakn;
import ai.grakn.GraknComputer;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Resource;
import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import ai.grakn.exception.GraknLockingException;
import ai.grakn.util.Schema;
import mjson.Json;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This is a task meant to be run periodically to eliminate resource duplication. The task performs a map-reduce job using 
 * the {@link ai.grakn.GraknComputer} where each resource index key, i.e. the resource value+type combination, is mapped 
 * to all its corresponding resource instances. The reduction steps, delete superfluous duplicates and forces instances
 * referring to them to point to the unique remaining resource instance for that key.
 * </p>
 * 
 * @author borislav
 *
 */
public class ResourceDeduplicationTask implements BackgroundTask {
    
    public static final String KEYSPACE_CONFIG  = "keyspace";
    public static final String KEYSPACE_DEFAULT = "grakn"; 
    public static final String DELETE_UNATTACHED_CONFIG  = "deleteUnattached";
    public static final boolean DELETE_UNATTACHED_DEFAULT = false; 
    
    private static final Logger LOG = LoggerFactory.getLogger(ResourceDeduplicationTask.class);
    private Long totalEliminated = null;
    
    static <T> T transact(GraknSession factory, Function<GraknGraph, T> work, String description) {
        while (true) {
            try (GraknGraph graph = factory.open(GraknTxType.WRITE)) {
                return work.apply(graph);
            }
            catch (GraknLockingException ex) {
                // Ignore - this exception means we must eventually succeed.
            }
            catch (Throwable t) {
                LOG.error("ResourceDeduplicationTask, while " + description, t);
                return null;
            }
        }
    }
    
    /**
     * The map-reduce job submitted to the GraknGraphComputer that scan the whole set of resources in the graph and
     * reduces by eliminating duplicates. The key in the mapping phase is formed by a resource's value and it's 
     * resource type.
     * 
     * @author borislav
     *
     */
    public static class Job implements MapReduce<String, ConceptId, String, Long, Long> {
        static Schema.ConceptProperty [] possibleValues = 
            {Schema.ConceptProperty.VALUE_BOOLEAN, Schema.ConceptProperty.VALUE_DOUBLE, Schema.ConceptProperty.VALUE_FLOAT,
             Schema.ConceptProperty.VALUE_INTEGER, Schema.ConceptProperty.VALUE_LONG, Schema.ConceptProperty.VALUE_STRING};
        
        private boolean deleteUnattached = false;
        private String keyspace;
        
        /**
         * Specify the keyspace to use for the deduplication job.
         */
        public Job keyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }
        
        /**
         * Specify whether resources that are not associated with any entity or relationship should be
         * deleted from the database.
         */
        public Job deleteUnattached(boolean deleteUnattached) {
            this.deleteUnattached = deleteUnattached;
            return this;
        }
        
        /**
         * Return <code>true</code> if this job will delete unattached resources and <code>false</code>
         * otherwise.
         */
        public boolean deleteUnattached() {
            return this.deleteUnattached;
        }
        
        /**
         * Emit the resoucre index (value + type) mapped to the concept ID of the resource instance.
         */
        public final void map(Vertex vertex, MapEmitter<String, ConceptId> emitter) {
            if (Schema.BaseType.valueOf(vertex.label()) != Schema.BaseType.RESOURCE) {
                return;
            }
            // We form the key from the resource type name and the value, and the value of the map-reduce is the concept ID
            // of the resource instance itself
            LOG.debug("Resource index: " + vertex.property(Schema.ConceptProperty.INDEX.name()).value());            
            Object key = vertex.property(Schema.ConceptProperty.INDEX.name()).value();
            if (key != null) {
                LOG.debug("Emit " + key + " -- "  +  ConceptId.of(vertex.property(Schema.ConceptProperty.ID.name())));
                emitter.emit(key.toString(), ConceptId.of(vertex.property(Schema.ConceptProperty.ID.name()).value()));
            }
            else {
                LOG.warn("Resource " + vertex.property(Schema.ConceptProperty.ID.name()) + " has no value?!");
            }
        }

        /**
         * We skip the combine stage and do only MAP and REDUCE. There is no optimization worth doing 
         * in a combine. The number of duplicates is not expected to be unmanageably large.
         */
        @Override
        public boolean doStage(Stage stage) { 
            return stage == Stage.MAP || stage == Stage.REDUCE;
        }
        
        /**
         * Here we simply collect all concepts for a key and ask our concept fixer to do its thing.
         */
        @Override
        public void reduce(String key, 
                           Iterator<ConceptId> values,
                           ReduceEmitter<String, Long> emitter) {
            LOG.debug("Reduce on " + key);
            HashSet<ConceptId> conceptIds = new HashSet<ConceptId>();
            while (values.hasNext()) {
                ConceptId current = values.next();
                conceptIds.add(current);
            }
            LOG.debug("Concepts: " + conceptIds);
            if (conceptIds.size() > 1) {
                // TODO: what if we fail here due to some read-write conflict?
                transact(Grakn.session(Grakn.DEFAULT_URI, keyspace),
                         (graph) -> ConceptFixer.runResourceFix(graph, key, conceptIds),
                         "Reducing resource duplicate set " + conceptIds);
                emitter.emit(key, (long) (conceptIds.size() - 1));
            }
            // Check and maybe delete resource if it's not attached to anything
            if (this.deleteUnattached ) {
                // TODO: what if we fail here due to some read-write conflict?
                try (GraknGraph graph = Grakn.session(Grakn.DEFAULT_URI, keyspace).open(GraknTxType.WRITE)) {
                    Resource<?> res = graph.admin().getConcept(Schema.ConceptProperty.INDEX, key);
                    if (res.ownerInstances().isEmpty() && res.relations().isEmpty()) {
                        res.delete();
                    }
                }
            }
        }

        @Override
        public String getMemoryKey() {
            // TODO: this should probably be the task ID, however how does a task get its ID?
            return ResourceDeduplicationTask.Job.class.getName(); 
        }

        @Override
        public MapReduce<String, ConceptId, String, Long, Long> clone() {
            return this;
        }

        @Override
        public Long generateFinalResult(Iterator<KeyValue<String, Long>> keyValues) {
            return IteratorUtils.reduce(keyValues, 0l, (a,b) -> a + b.getValue());
        }
    }
    
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoin, Json configuration) {
        LOG.info("Starting ResourceDeduplicationTask : " + configuration);
        
        String keyspace = configuration.at("keyspace", KEYSPACE_DEFAULT).asString();
        GraknComputer computer = Grakn.session(Grakn.DEFAULT_URI, keyspace).getGraphComputer();
        Job job = new Job().keyspace(keyspace)
                           .deleteUnattached(configuration.at("deletedUnattached", DELETE_UNATTACHED_DEFAULT ).asBoolean());
        this.totalEliminated = computer.compute(job).memory().get(job.getMemoryKey());
        return true;
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean stop() {
        return true;
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    public Long totalElimintated() {
        return this.totalEliminated;
    }
}
