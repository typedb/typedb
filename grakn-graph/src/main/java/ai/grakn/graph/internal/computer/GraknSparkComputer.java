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

package ai.grakn.graph.internal.computer;

import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.storage.StorageLevel;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.AbstractHadoopGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.util.ComputerSubmissionHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorage;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputOutputHelper;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedInputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.SparkContextStorage;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * <p>
 * This is a modified version of Spark Computer.
 * We change its behaviour so it can won't destroy the rdd after every job.
 * </p>
 *
 * @author Jason Liu
 * @author Marko A. Rodriguez
 */
public final class GraknSparkComputer extends AbstractHadoopGraphComputer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraknSparkComputer.class);

    private final org.apache.commons.configuration.Configuration sparkConfiguration;
    private boolean workersSet = false;

    private static GraknGraphRDD graknGraphRDD = null;

    private org.apache.commons.configuration.Configuration apacheConfiguration = null;
    private Configuration hadoopConfiguration = null;

    private String jobGroupId = null;

    public GraknSparkComputer(final HadoopGraph hadoopGraph) {
        super(hadoopGraph);
        this.sparkConfiguration = new HadoopConfiguration();
        ConfigurationUtils.copy(this.hadoopGraph.configuration(), this.sparkConfiguration);

        this.apacheConfiguration = new HadoopConfiguration(this.sparkConfiguration);
        apacheConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT_HAS_EDGES, false);
        hadoopConfiguration = ConfUtil.makeHadoopConfiguration(apacheConfiguration);

        if (hadoopConfiguration.get(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, null) == null &&
                hadoopConfiguration.get(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, null) != null &&
                FileInputFormat.class.isAssignableFrom(hadoopConfiguration
                        .getClass(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class))) {
            try {
                final String inputLocation = FileSystem.get(hadoopConfiguration)
                        .getFileStatus(new Path(hadoopConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION)))
                        .getPath().toString();
                apacheConfiguration.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputLocation);
                hadoopConfiguration.set(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR, inputLocation);
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    @Override
    public GraphComputer workers(final int workers) {
        super.workers(workers);
        if (this.sparkConfiguration.containsKey(SparkLauncher.SPARK_MASTER) &&
                this.sparkConfiguration.getString(SparkLauncher.SPARK_MASTER).startsWith("local")) {
            this.sparkConfiguration.setProperty(SparkLauncher.SPARK_MASTER, "local[" + this.workers + "]");
        }
        this.workersSet = true;
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        this.validateStatePriorToExecution();

        return ComputerSubmissionHelper
                .runWithBackgroundThread(this::submitWithExecutor, "SparkSubmitter");
    }

    public void cancelJobs() {
        if (jobGroupId != null && graknGraphRDD != null && graknGraphRDD.sparkContext != null) {
            graknGraphRDD.sparkContext.cancelJobGroup(jobGroupId);
        }
    }

    private Future<ComputerResult> submitWithExecutor(Executor exec) {
        getGraphRDD(this);
        jobGroupId = Integer.toString(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
        String jobDescription = this.vertexProgram == null ? this.mapReducers.toString() :
                this.vertexProgram + "+" + this.mapReducers;

        this.sparkConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION,
                this.sparkConfiguration.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + jobGroupId);
        this.apacheConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION,
                this.sparkConfiguration.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION));
        this.hadoopConfiguration.set(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION,
                this.sparkConfiguration.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION));

        // create the completable future
        return CompletableFuture.supplyAsync(() -> {
            graknGraphRDD.sparkContext.setJobGroup(jobGroupId, jobDescription);
            final long startTime = System.currentTimeMillis();

            GraknSparkMemory memory = null;
            JavaPairRDD<Object, VertexWritable> computedGraphRDD = null;
            JavaPairRDD<Object, ViewIncomingPayload<Object>> viewIncomingRDD = null;

            ////////////////////////////////
            // process the vertex program //
            ////////////////////////////////
            if (null != this.vertexProgram) {
                // set up the vertex program and wire up configurations
                this.mapReducers.addAll(this.vertexProgram.getMapReducers());
                memory = new GraknSparkMemory(this.vertexProgram, this.mapReducers, graknGraphRDD.sparkContext);
                this.vertexProgram.setup(memory);
                memory.broadcastMemory(graknGraphRDD.sparkContext);
                final HadoopConfiguration vertexProgramConfiguration = new HadoopConfiguration();
                this.vertexProgram.storeState(vertexProgramConfiguration);
                ConfigurationUtils.copy(vertexProgramConfiguration, apacheConfiguration);
                ConfUtil.mergeApacheIntoHadoopConfiguration(vertexProgramConfiguration, hadoopConfiguration);
                // execute the vertex program
                while (true) {
                    memory.setInTask(true);
                    viewIncomingRDD = GraknSparkExecutor.executeVertexProgramIteration(
                            graknGraphRDD.loadedGraphRDD, viewIncomingRDD, memory, vertexProgramConfiguration);
                    memory.setInTask(false);
                    if (this.vertexProgram.terminate(memory)) break;
                    else {
                        memory.incrIteration();
                        memory.broadcastMemory(graknGraphRDD.sparkContext);
                    }
                }
                // write the computed graph to the respective output (rdd or output format)
                final String[] elementComputeKeys = this.vertexProgram.getElementComputeKeys().toArray(
                        new String[this.vertexProgram.getElementComputeKeys().size()]);
                computedGraphRDD = GraknSparkExecutor.prepareFinalGraphRDD(
                        graknGraphRDD.loadedGraphRDD, viewIncomingRDD, elementComputeKeys);
                if ((hadoopConfiguration.get(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, null) != null ||
                        hadoopConfiguration.get(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, null) != null) &&
                        !this.persist.equals(Persist.NOTHING)) {
                    try {
                        hadoopConfiguration
                                .getClass(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD,
                                        OutputFormatRDD.class, OutputRDD.class)
                                .newInstance()
                                .writeGraphRDD(apacheConfiguration, computedGraphRDD);
                    } catch (final InstantiationException | IllegalAccessException e) {
                        throw new IllegalStateException(e.getMessage(), e);
                    }
                }
            }

            final boolean computedGraphCreated = computedGraphRDD != null;
            if (!computedGraphCreated) {
                computedGraphRDD = graknGraphRDD.loadedGraphRDD;
            }

            final Memory.Admin finalMemory = null == memory ? new MapMemory() : new MapMemory(memory);

            //////////////////////////////
            // process the map reducers //
            //////////////////////////////
            if (!this.mapReducers.isEmpty()) {
                for (final MapReduce mapReduce : this.mapReducers) {
                    // execute the map reduce job
                    final HadoopConfiguration newApacheConfiguration = new HadoopConfiguration(apacheConfiguration);
                    mapReduce.storeState(newApacheConfiguration);
                    // map
                    final JavaPairRDD mapRDD = GraknSparkExecutor
                            .executeMap(computedGraphRDD, mapReduce, newApacheConfiguration);
                    // combine
                    final JavaPairRDD combineRDD = mapReduce.doStage(MapReduce.Stage.COMBINE) ?
                            GraknSparkExecutor.executeCombine(mapRDD, newApacheConfiguration) : mapRDD;
                    // reduce
                    final JavaPairRDD reduceRDD = mapReduce.doStage(MapReduce.Stage.REDUCE) ?
                            GraknSparkExecutor.executeReduce(combineRDD, mapReduce, newApacheConfiguration) : combineRDD;
                    // write the map reduce output back to disk and computer result memory
                    try {
                        mapReduce.addResultToMemory(finalMemory, hadoopConfiguration
                                .getClass(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD,
                                        OutputFormatRDD.class, OutputRDD.class)
                                .newInstance()
                                .writeMemoryRDD(apacheConfiguration, mapReduce.getMemoryKey(), reduceRDD));
                    } catch (final InstantiationException | IllegalAccessException e) {
                        throw new IllegalStateException(e.getMessage(), e);
                    }
                }
            }

            // unpersist the computed graph if it will not be used again (no PersistedOutputRDD)
            if (!graknGraphRDD.outputToSpark || this.persist.equals(GraphComputer.Persist.NOTHING)) {
                computedGraphRDD.unpersist();
            }
            // delete any file system or rdd data if persist nothing
            String outputPath = sparkConfiguration.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION);
            if (null != outputPath && this.persist.equals(GraphComputer.Persist.NOTHING)) {
                if (graknGraphRDD.outputToHDFS) {
                    graknGraphRDD.fileSystemStorage.rm(outputPath);
                }
                if (graknGraphRDD.outputToSpark) {
                    graknGraphRDD.sparkContextStorage.rm(outputPath);
                }
            }
            // update runtime and return the newly computed graph
            finalMemory.setRuntime(System.currentTimeMillis() - startTime);
            return new DefaultComputerResult(InputOutputHelper.getOutputGraph(
                    apacheConfiguration, this.resultGraph, this.persist), finalMemory.asImmutable());
        }, exec);
    }

    /////////////////

    private static void loadJars(final JavaSparkContext sparkContext,
                                 final Configuration hadoopConfiguration) {
        if (hadoopConfiguration.getBoolean(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true)) {
            final String hadoopGremlinLocalLibs = null == System.getProperty(Constants.HADOOP_GREMLIN_LIBS) ?
                    System.getenv(Constants.HADOOP_GREMLIN_LIBS) : System.getProperty(Constants.HADOOP_GREMLIN_LIBS);
            if (null == hadoopGremlinLocalLibs) {
                LOGGER.warn(Constants.HADOOP_GREMLIN_LIBS + " is not set -- proceeding regardless");
            } else {
                final String[] paths = hadoopGremlinLocalLibs.split(":");
                for (final String path : paths) {
                    final File file = new File(path);
                    if (file.exists()) {
                        Stream.of(file.listFiles())
                                .filter(f -> f.getName().endsWith(Constants.DOT_JAR))
                                .forEach(f -> sparkContext.addJar(f.getAbsolutePath()));
                    } else {
                        LOGGER.warn(path + " does not reference a valid directory -- proceeding regardless");
                    }
                }
            }
        }
    }

    /**
     * When using a persistent context the running Context's configuration will override a passed
     * in configuration. Spark allows us to override these inherited properties via
     * SparkContext.setLocalProperty
     */
    private static void updateLocalConfiguration(final JavaSparkContext sparkContext,
                                                 final SparkConf sparkConfiguration) {
        /*
         * While we could enumerate over the entire SparkConfiguration and copy into the Thread
         * Local properties of the Spark Context this could cause adverse effects with future
         * versions of Spark. Since the api for setting multiple local properties at once is
         * restricted as private, we will only set those properties we know can effect SparkGraphComputer
         * Execution rather than applying the entire configuration.
         */
        final String[] validPropertyNames = {
                "spark.job.description",
                "spark.jobGroup.id",
                "spark.job.interruptOnCancel",
                "spark.scheduler.pool"
        };

        for (String propertyName : validPropertyNames) {
            if (sparkConfiguration.contains(propertyName)) {
                String propertyValue = sparkConfiguration.get(propertyName);
                LOGGER.info("Setting Thread Local SparkContext Property - "
                        + propertyName + " : " + propertyValue);

                sparkContext.setLocalProperty(propertyName, sparkConfiguration.get(propertyName));
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final FileConfiguration configuration = new PropertiesConfiguration(args[0]);
        new GraknSparkComputer(HadoopGraph.open(configuration))
                .program(VertexProgram.createVertexProgram(HadoopGraph.open(configuration), configuration))
                .submit().get();
    }

    private static synchronized void getGraphRDD(GraknSparkComputer graknSparkComputer) {
        if (graknGraphRDD == null || GraknGraphRDD.commit || graknGraphRDD.sparkContext == null) {
            LOGGER.info("Creating a new Grakn Graph RDD");
            graknGraphRDD = new GraknGraphRDD(graknSparkComputer);
        }
    }

    public static void refresh() {
        if (!GraknGraphRDD.commit) {
            setCommitFlag();
        }
    }

    private static synchronized void setCommitFlag() {
        if (!GraknGraphRDD.commit) {
            GraknGraphRDD.commit = true;
        }
    }

    public static synchronized void clear() {
        if (graknGraphRDD != null) {
            graknGraphRDD.loadedGraphRDD = null;
            graknGraphRDD = null;
        }
        Spark.close();
    }

    private static class GraknGraphRDD {

        private static boolean commit = false;

        private Storage fileSystemStorage;
        private Storage sparkContextStorage;

        private boolean outputToHDFS;
        private boolean outputToSpark;

        private String outputLocation;

        private SparkConf sparkConf;
        private JavaSparkContext sparkContext;

        private JavaPairRDD<Object, VertexWritable> loadedGraphRDD;

        private boolean inputFromSpark;

        private GraknGraphRDD(GraknSparkComputer graknSparkComputer) {

            fileSystemStorage = FileSystemStorage.open(graknSparkComputer.hadoopConfiguration);
            sparkContextStorage = SparkContextStorage.open(graknSparkComputer.apacheConfiguration);

            inputFromSpark = PersistedInputRDD.class.isAssignableFrom(graknSparkComputer.
                    hadoopConfiguration.getClass(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, Object.class));
            outputToHDFS = FileOutputFormat.class.isAssignableFrom(graknSparkComputer.
                    hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, Object.class));
            outputToSpark = PersistedOutputRDD.class.isAssignableFrom(graknSparkComputer.
                    hadoopConfiguration.getClass(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, Object.class));

            // delete output location
            outputLocation = graknSparkComputer.hadoopConfiguration
                    .get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null);
            if (null != outputLocation) {
                if (outputToHDFS && fileSystemStorage.exists(outputLocation)) {
                    fileSystemStorage.rm(outputLocation);
                }
                if (outputToSpark && sparkContextStorage.exists(outputLocation)) {
                    sparkContextStorage.rm(outputLocation);
                }
            }

            // wire up a spark context
            sparkConf = new SparkConf();
            sparkConf.setAppName(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX);

            // create the spark configuration from the graph computer configuration
            graknSparkComputer.hadoopConfiguration.forEach(entry -> sparkConf.set(entry.getKey(), entry.getValue()));

            sparkContext = new JavaSparkContext(SparkContext.getOrCreate(sparkConf));
            loadJars(sparkContext, graknSparkComputer.hadoopConfiguration);
            Spark.create(sparkContext.sc()); // this is the context RDD holder that prevents GC
            updateLocalConfiguration(sparkContext, sparkConf);

            boolean partitioned = false;
            try {
                loadedGraphRDD = graknSparkComputer.hadoopConfiguration
                        .getClass(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, InputFormatRDD.class, InputRDD.class)
                        .newInstance()
                        .readGraphRDD(graknSparkComputer.apacheConfiguration, sparkContext);

                if (loadedGraphRDD.partitioner().isPresent()) {
                    LOGGER.info(
                            "Using the existing partitioner associated with the loaded graphRDD: " +
                                    loadedGraphRDD.partitioner().get());
                } else {
                    loadedGraphRDD = loadedGraphRDD.partitionBy(new HashPartitioner(graknSparkComputer.workersSet ?
                            graknSparkComputer.workers : loadedGraphRDD.partitions().size()));
                    partitioned = true;
                }
                assert loadedGraphRDD.partitioner().isPresent();
                if (graknSparkComputer.workersSet) {
                    // ensures that the loaded graphRDD does not have more partitions than workers
                    if (loadedGraphRDD.partitions().size() > graknSparkComputer.workers) {
                        loadedGraphRDD = loadedGraphRDD.coalesce(graknSparkComputer.workers);
                    } else if (loadedGraphRDD.partitions().size() < graknSparkComputer.workers) {
                        loadedGraphRDD = loadedGraphRDD.repartition(graknSparkComputer.workers);
                    }
                }
                if (!inputFromSpark || partitioned) {
                    loadedGraphRDD = loadedGraphRDD.persist(
                            StorageLevel.fromString(graknSparkComputer.hadoopConfiguration
                                    .get(Constants.GREMLIN_SPARK_GRAPH_STORAGE_LEVEL, "MEMORY_ONLY")));
                }
                GraknGraphRDD.commit = false;
            } catch (final InstantiationException | IllegalAccessException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
