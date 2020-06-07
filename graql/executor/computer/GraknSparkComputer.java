/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.executor.computer;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.AbstractHadoopGraphComputer;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.util.ComputerSubmissionHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorage;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.GraphFilterAware;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolShimService;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.SparkInterceptorStrategy;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.SparkSingleIterationStrategy;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputOutputHelper;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputFormatRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.OutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedInputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.SparkContextStorage;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoRegistrator;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.kryoshim.unshaded.UnshadedKryoShimService;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShimServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;

/**
 * <p>
 * This is a modified version of Spark Computer.
 * We change its behaviour so it can won't destroy the rdd after every job.
 * </p>
 */
public final class GraknSparkComputer extends AbstractHadoopGraphComputer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraknSparkComputer.class);

    private final org.apache.commons.configuration.Configuration sparkConfiguration;
    private boolean workersSet = false;
    private final ThreadFactory threadFactoryBoss =
            new BasicThreadFactory.Builder().namingPattern(GraknSparkComputer.class.getSimpleName() + "-boss").build();

    private static final Set<String> KEYS_PASSED_IN_JVM_SYSTEM_PROPERTIES = new HashSet<>(Arrays.asList(
            KryoShimServiceLoader.KRYO_SHIM_SERVICE,
            IoRegistry.IO_REGISTRY));

    private final ExecutorService computerService = Executors.newSingleThreadExecutor(threadFactoryBoss);

    static {
        TraversalStrategies.GlobalCache.registerStrategies(GraknSparkComputer.class,
                TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone().addStrategies(
                        SparkSingleIterationStrategy.instance(),
                        SparkInterceptorStrategy.instance()));
    }

    private String jobGroupId = null;

    public GraknSparkComputer(HadoopGraph hadoopGraph) {
        super(hadoopGraph);
        this.sparkConfiguration = new HadoopConfiguration();
        ConfigurationUtils.copy(this.hadoopGraph.configuration(), this.sparkConfiguration);
    }

    @Override
    public GraphComputer workers(int workers) {
        super.workers(workers);
        if (this.sparkConfiguration.containsKey(SparkLauncher.SPARK_MASTER) &&
                this.sparkConfiguration.getString(SparkLauncher.SPARK_MASTER).startsWith("local")) {
            this.sparkConfiguration.setProperty(SparkLauncher.SPARK_MASTER, "local[" + this.workers + "]");
        }
        this.workersSet = true;
        return this;
    }

    @Override
    public GraphComputer configure(String key, Object value) {
        this.sparkConfiguration.setProperty(key, value);
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        this.validateStatePriorToExecution();

        return ComputerSubmissionHelper
                .runWithBackgroundThread(exec -> submitWithExecutor(), "SparkSubmitter");
    }

    public void cancelJobs() {
        if (jobGroupId != null) {
            Spark.getContext().cancelJobGroup(jobGroupId);
        }
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private Future<ComputerResult> submitWithExecutor() {
        jobGroupId = Integer.toString(ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
        String jobDescription = this.vertexProgram == null ? this.mapReducers.toString() :
                this.vertexProgram + "+" + this.mapReducers;

        // Use different output locations
        this.sparkConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION,
                this.sparkConfiguration.getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION) + "/" + jobGroupId);

        updateConfigKeys(sparkConfiguration);

        Future<ComputerResult> result = computerService.submit(() -> {
            long startTime = System.currentTimeMillis();

            //////////////////////////////////////////////////
            /////// PROCESS SHIM AND SYSTEM PROPERTIES ///////
            //////////////////////////////////////////////////
            String shimService = KryoSerializer.class.getCanonicalName().equals(this.sparkConfiguration.getString(Constants.SPARK_SERIALIZER, null)) ?
                    UnshadedKryoShimService.class.getCanonicalName() :
                    HadoopPoolShimService.class.getCanonicalName();
            this.sparkConfiguration.setProperty(KryoShimServiceLoader.KRYO_SHIM_SERVICE, shimService);
            ///////////
            StringBuilder params = new StringBuilder();
            this.sparkConfiguration.getKeys().forEachRemaining(key -> {
                if (KEYS_PASSED_IN_JVM_SYSTEM_PROPERTIES.contains(key)) {
                    params.append(" -D").append("tinkerpop.").append(key).append("=").append(this.sparkConfiguration.getProperty(key));
                    System.setProperty("tinkerpop." + key, this.sparkConfiguration.getProperty(key).toString());
                }
            });
            if (params.length() > 0) {
                this.sparkConfiguration.setProperty(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS,
                        (this.sparkConfiguration.getString(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, "") + params.toString()).trim());
                this.sparkConfiguration.setProperty(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS,
                        (this.sparkConfiguration.getString(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "") + params.toString()).trim());
            }
            KryoShimServiceLoader.applyConfiguration(this.sparkConfiguration);
            //////////////////////////////////////////////////
            //////////////////////////////////////////////////
            //////////////////////////////////////////////////
            // apache and hadoop configurations that are used throughout the graph computer computation
            org.apache.commons.configuration.Configuration graphComputerConfiguration =
                    new HadoopConfiguration(this.sparkConfiguration);
            if (!graphComputerConfiguration.containsKey(Constants.SPARK_SERIALIZER)) {
                graphComputerConfiguration.setProperty(Constants.SPARK_SERIALIZER, KryoSerializer.class.getCanonicalName());
                if (!graphComputerConfiguration.containsKey(Constants.SPARK_KRYO_REGISTRATOR)) {
                    graphComputerConfiguration.setProperty(Constants.SPARK_KRYO_REGISTRATOR, GryoRegistrator.class.getCanonicalName());
                }
            }
            graphComputerConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER_HAS_EDGES,
                    this.persist.equals(GraphComputer.Persist.EDGES));

            Configuration hadoopConfiguration = ConfUtil.makeHadoopConfiguration(graphComputerConfiguration);

            Storage fileSystemStorage = FileSystemStorage.open(hadoopConfiguration);
            boolean inputFromHDFS = FileInputFormat.class.isAssignableFrom(
                    hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_READER, Object.class));
            boolean inputFromSpark = PersistedInputRDD.class.isAssignableFrom(
                    hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_READER, Object.class));
            boolean outputToHDFS = FileOutputFormat.class.isAssignableFrom(
                    hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_WRITER, Object.class));
            boolean outputToSpark = PersistedOutputRDD.class.isAssignableFrom(
                    hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_WRITER, Object.class));
            boolean skipPartitioner = graphComputerConfiguration.getBoolean(
                    Constants.GREMLIN_SPARK_SKIP_PARTITIONER, false);
            boolean skipPersist = graphComputerConfiguration.getBoolean(
                    Constants.GREMLIN_SPARK_SKIP_GRAPH_CACHE, false);

            if (inputFromHDFS) {
                String inputLocation = Constants
                        .getSearchGraphLocation(
                                hadoopConfiguration.get(Constants.GREMLIN_HADOOP_INPUT_LOCATION),
                                fileSystemStorage)
                        .orElse(null);
                if (null != inputLocation) {
                    try {
                        graphComputerConfiguration.setProperty(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR,
                                FileSystem.get(hadoopConfiguration).getFileStatus(new Path(inputLocation)).getPath()
                                        .toString());
                        hadoopConfiguration.set(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUTDIR,
                                FileSystem.get(hadoopConfiguration).getFileStatus(new Path(inputLocation)).getPath()
                                        .toString());
                    } catch (IOException e) {
                        throw new IllegalStateException(e.getMessage(), e);
                    }
                }
            }

            InputRDD inputRDD;
            OutputRDD outputRDD;
            boolean filtered;
            try {
                inputRDD = InputRDD.class.isAssignableFrom(
                        hadoopConfiguration.getClass(
                                Constants.GREMLIN_HADOOP_GRAPH_READER, Object.class)) ?
                        hadoopConfiguration.getClass(
                                Constants.GREMLIN_HADOOP_GRAPH_READER, InputRDD.class, InputRDD.class).newInstance() :
                        InputFormatRDD.class.newInstance();
                outputRDD = OutputRDD.class.isAssignableFrom(
                        hadoopConfiguration.getClass(
                                Constants.GREMLIN_HADOOP_GRAPH_WRITER, Object.class)) ?
                        hadoopConfiguration.getClass(
                                Constants.GREMLIN_HADOOP_GRAPH_WRITER, OutputRDD.class, OutputRDD.class).newInstance() :
                        OutputFormatRDD.class.newInstance();

                // if the input class can filter on load, then set the filters
                if (inputRDD instanceof InputFormatRDD &&
                        GraphFilterAware.class.isAssignableFrom(hadoopConfiguration.getClass(
                                Constants.GREMLIN_HADOOP_GRAPH_READER, InputFormat.class, InputFormat.class))) {
                    GraphFilterAware.storeGraphFilter(
                            graphComputerConfiguration, hadoopConfiguration, this.graphFilter);
                    filtered = false;
                } else if (inputRDD instanceof GraphFilterAware) {
                    ((GraphFilterAware) inputRDD).setGraphFilter(this.graphFilter);
                    filtered = false;
                } else filtered = this.graphFilter.hasFilter();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }

            // create the spark context from the graph computer configuration
            JavaSparkContext sparkContext = new JavaSparkContext(Spark.create(hadoopConfiguration));
            Storage sparkContextStorage = SparkContextStorage.open();

            sparkContext.setJobGroup(jobGroupId, jobDescription);

            GraknSparkMemory memory = null;
            // delete output location
            String outputLocation = hadoopConfiguration.get(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null);
            if (null != outputLocation) {
                if (outputToHDFS && fileSystemStorage.exists(outputLocation)) {
                    fileSystemStorage.rm(outputLocation);
                }
                if (outputToSpark && sparkContextStorage.exists(outputLocation)) {
                    sparkContextStorage.rm(outputLocation);
                }
            }

            // the Spark application name will always be set by SparkContextStorage,
            // thus, INFO the name to make it easier to debug
            logger.debug(Constants.GREMLIN_HADOOP_SPARK_JOB_PREFIX +
                    (null == this.vertexProgram ? "No VertexProgram" :
                            this.vertexProgram) + "[" + this.mapReducers + "]");

            // add the project jars to the cluster
            this.loadJars(hadoopConfiguration, sparkContext);
            updateLocalConfiguration(sparkContext, hadoopConfiguration);

            // create a message-passing friendly rdd from the input rdd
            boolean partitioned = false;
            JavaPairRDD<Object, VertexWritable> loadedGraphRDD =
                    inputRDD.readGraphRDD(graphComputerConfiguration, sparkContext);

            // if there are vertex or edge filters, filter the loaded graph rdd prior to partitioning and persisting
            if (filtered) {
                this.logger.debug("Filtering the loaded graphRDD: " + this.graphFilter);
                loadedGraphRDD = GraknSparkExecutor.applyGraphFilter(loadedGraphRDD, this.graphFilter);
            }
            // if the loaded graph RDD is already partitioned use that partitioner,
            // else partition it with HashPartitioner
            if (loadedGraphRDD.partitioner().isPresent()) {
                this.logger.debug("Using the existing partitioner associated with the loaded graphRDD: " +
                        loadedGraphRDD.partitioner().get());
            } else {
                if (!skipPartitioner) {
                    Partitioner partitioner =
                            new HashPartitioner(this.workersSet ?
                                    this.workers : loadedGraphRDD.partitions().size());
                    logger.debug("Partitioning the loaded graphRDD: " + partitioner);
                    loadedGraphRDD = loadedGraphRDD.partitionBy(partitioner);
                    partitioned = true;
                    Preconditions.checkState(loadedGraphRDD.partitioner().isPresent());
                } else {
                    // no easy way to test this with a test case
                    Preconditions.checkState(skipPartitioner == !loadedGraphRDD.partitioner().isPresent());

                    logger.debug("Partitioning has been skipped for the loaded graphRDD via " +
                            Constants.GREMLIN_SPARK_SKIP_PARTITIONER);
                }
            }
            // if the loaded graphRDD was already partitioned previous,
            // then this coalesce/repartition will not take place
            if (this.workersSet) {
                // ensures that the loaded graphRDD does not have more partitions than workers
                if (loadedGraphRDD.partitions().size() > this.workers) {
                    loadedGraphRDD = loadedGraphRDD.coalesce(this.workers);
                } else {
                    // ensures that the loaded graphRDD does not have less partitions than workers
                    if (loadedGraphRDD.partitions().size() < this.workers) {
                        loadedGraphRDD = loadedGraphRDD.repartition(this.workers);
                    }
                }
            }
            // persist the vertex program loaded graph as specified by configuration
            // or else use default cache() which is MEMORY_ONLY
            if (!skipPersist && (!inputFromSpark || partitioned || filtered)) {
                loadedGraphRDD = loadedGraphRDD.persist(StorageLevel.fromString(hadoopConfiguration.get(
                        Constants.GREMLIN_SPARK_GRAPH_STORAGE_LEVEL, "MEMORY_ONLY")));
            }
            // final graph with view
            // (for persisting and/or mapReducing -- may be null and thus, possible to save space/time)
            JavaPairRDD<Object, VertexWritable> computedGraphRDD = null;
            try {
                ////////////////////////////////
                // process the vertex program //
                ////////////////////////////////
                if (null != this.vertexProgram) {
                    memory = new GraknSparkMemory(this.vertexProgram, this.mapReducers, sparkContext);
                    /////////////////
                    // if there is a registered VertexProgramInterceptor, use it to bypass the GraphComputer semantics
                    if (graphComputerConfiguration.containsKey(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR)) {
                        try {
                            GraknSparkVertexProgramInterceptor<VertexProgram> interceptor =
                                    (GraknSparkVertexProgramInterceptor)
                                            Class.forName(graphComputerConfiguration.getString(
                                                    Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR)).newInstance();
                            computedGraphRDD = interceptor.apply(this.vertexProgram, loadedGraphRDD, memory);
                        } catch (final ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                            throw new IllegalStateException(e.getMessage());
                        }
                    } else {
                        // standard GraphComputer semantics
                        // get a configuration that will be propagated to all workers
                        HadoopConfiguration vertexProgramConfiguration = new HadoopConfiguration();
                        this.vertexProgram.storeState(vertexProgramConfiguration);
                        // set up the vertex program and wire up configurations
                        this.vertexProgram.setup(memory);
                        JavaPairRDD<Object, ViewIncomingPayload<Object>> viewIncomingRDD = null;
                        memory.broadcastMemory(sparkContext);
                        // execute the vertex program
                        while (true) {
                            if (Thread.interrupted()) {
                                sparkContext.cancelAllJobs();
                                throw new TraversalInterruptedException();
                            }
                            memory.setInExecute(true);
                            viewIncomingRDD =
                                    GraknSparkExecutor.executeVertexProgramIteration(
                                            loadedGraphRDD, viewIncomingRDD, memory,
                                            graphComputerConfiguration, vertexProgramConfiguration);
                            memory.setInExecute(false);
                            if (this.vertexProgram.terminate(memory)) {
                                break;
                            } else {
                                memory.incrIteration();
                                memory.broadcastMemory(sparkContext);
                            }
                        }
                        // if the graph will be continued to be used (persisted or mapreduced),
                        // then generate a view+graph
                        if ((null != outputRDD && !this.persist.equals(Persist.NOTHING)) ||
                                !this.mapReducers.isEmpty()) {
                            computedGraphRDD = GraknSparkExecutor.prepareFinalGraphRDD(
                                    loadedGraphRDD, viewIncomingRDD, this.vertexProgram.getVertexComputeKeys());
                            Preconditions.checkState(null != computedGraphRDD && computedGraphRDD != loadedGraphRDD);
                        }
                    }
                    /////////////////
                    memory.complete(); // drop all transient memory keys
                    // write the computed graph to the respective output (rdd or output format)
                    if (null != outputRDD && !this.persist.equals(Persist.NOTHING)) {
                        // the logic holds that a computeGraphRDD must be created at this point
                        Preconditions.checkNotNull(computedGraphRDD);

                        outputRDD.writeGraphRDD(graphComputerConfiguration, computedGraphRDD);
                    }
                }

                boolean computedGraphCreated = computedGraphRDD != null && computedGraphRDD != loadedGraphRDD;
                if (!computedGraphCreated) {
                    computedGraphRDD = loadedGraphRDD;
                }

                Memory.Admin finalMemory = null == memory ? new MapMemory() : new MapMemory(memory);

                //////////////////////////////
                // process the map reducers //
                //////////////////////////////
                if (!this.mapReducers.isEmpty()) {
                    // create a mapReduceRDD for executing the map reduce jobs on
                    JavaPairRDD<Object, VertexWritable> mapReduceRDD = computedGraphRDD;
                    if (computedGraphCreated && !outputToSpark) {
                        // drop all the edges of the graph as they are not used in mapReduce processing
                        mapReduceRDD = computedGraphRDD.mapValues(vertexWritable -> {
                            vertexWritable.get().dropEdges(Direction.BOTH);
                            return vertexWritable;
                        });
                        // if there is only one MapReduce to execute, don't bother wasting the clock cycles.
                        if (this.mapReducers.size() > 1) {
                            mapReduceRDD = mapReduceRDD.persist(StorageLevel.fromString(hadoopConfiguration.get(
                                    Constants.GREMLIN_SPARK_GRAPH_STORAGE_LEVEL, "MEMORY_ONLY")));
                        }
                    }

                    for (MapReduce mapReduce : this.mapReducers) {
                        // execute the map reduce job
                        HadoopConfiguration newApacheConfiguration =
                                new HadoopConfiguration(graphComputerConfiguration);
                        mapReduce.storeState(newApacheConfiguration);
                        // map
                        JavaPairRDD mapRDD = GraknSparkExecutor.executeMap(mapReduceRDD, mapReduce, newApacheConfiguration);
                        // combine
                        JavaPairRDD combineRDD = mapReduce.doStage(MapReduce.Stage.COMBINE) ?
                                GraknSparkExecutor.executeCombine(mapRDD, newApacheConfiguration) :
                                mapRDD;
                        // reduce
                        JavaPairRDD reduceRDD = mapReduce.doStage(MapReduce.Stage.REDUCE) ?
                                GraknSparkExecutor.executeReduce(combineRDD, mapReduce, newApacheConfiguration) :
                                combineRDD;
                        // write the map reduce output back to disk and computer result memory
                        if (null != outputRDD) {
                            mapReduce.addResultToMemory(finalMemory, outputRDD.writeMemoryRDD(
                                    graphComputerConfiguration, mapReduce.getMemoryKey(), reduceRDD));
                        }
                    }
                    // if the mapReduceRDD is not simply the computed graph, unpersist the mapReduceRDD
                    if (computedGraphCreated && !outputToSpark) {
                        Preconditions.checkState(loadedGraphRDD != computedGraphRDD);
                        Preconditions.checkState(mapReduceRDD != computedGraphRDD);
                        mapReduceRDD.unpersist();
                    } else {
                        Preconditions.checkState(mapReduceRDD == computedGraphRDD);
                    }
                }

                // unpersist the loaded graph if it will not be used again (no PersistedInputRDD)
                // if the graphRDD was loaded from Spark, but then partitioned or filtered, its a different RDD
                if (!inputFromSpark || partitioned || filtered) {
                    loadedGraphRDD.unpersist();
                }
                // unpersist the computed graph if it will not be used again (no PersistedOutputRDD)
                // if the computed graph is the loadedGraphRDD because it was not mutated and not-unpersisted,
                // then don't unpersist the computedGraphRDD/loadedGraphRDD
                if ((!outputToSpark || this.persist.equals(GraphComputer.Persist.NOTHING)) && computedGraphCreated) {
                    computedGraphRDD.unpersist();
                }
                // delete any file system or rdd data if persist nothing
                if (null != outputLocation && this.persist.equals(GraphComputer.Persist.NOTHING)) {
                    if (outputToHDFS) {
                        fileSystemStorage.rm(outputLocation);
                    }
                    if (outputToSpark) {
                        sparkContextStorage.rm(outputLocation);
                    }
                }
                // update runtime and return the newly computed graph
                finalMemory.setRuntime(System.currentTimeMillis() - startTime);
                // clear properties that should not be propagated in an OLAP chain
                graphComputerConfiguration.clearProperty(Constants.GREMLIN_HADOOP_GRAPH_FILTER);
                graphComputerConfiguration.clearProperty(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR);
                graphComputerConfiguration.clearProperty(Constants.GREMLIN_SPARK_SKIP_GRAPH_CACHE);
                graphComputerConfiguration.clearProperty(Constants.GREMLIN_SPARK_SKIP_PARTITIONER);
                return new DefaultComputerResult(
                        InputOutputHelper.getOutputGraph(graphComputerConfiguration, this.resultGraph, this.persist),
                        finalMemory.asImmutable());
            } catch (Exception e) {
                // So it throws the same exception as tinker does
                throw new RuntimeException(e);
            }
        });
        computerService.shutdown();
        return result;
    }

    private static void updateConfigKeys(org.apache.commons.configuration.Configuration sparkConfiguration) {
        Set<String> wrongKeys = new HashSet<>();
        sparkConfiguration.getKeys().forEachRemaining(wrongKeys::add);
        wrongKeys.forEach(key -> {
            if (key.startsWith("janusmr")) {
                String newKey = "janusgraphmr" + key.substring(7);
                sparkConfiguration.setProperty(newKey, sparkConfiguration.getString(key));
            }
        });
    }

    /////////////////

    @Override
    protected void loadJar(final Configuration hadoopConfiguration, final File file, final Object... params) {
        JavaSparkContext sparkContext = (JavaSparkContext) params[0];
        sparkContext.addJar(file.getAbsolutePath());
    }

    /**
     * When using a persistent context the running Context's configuration will override a passed
     * in configuration. Spark allows us to override these inherited properties via
     * SparkContext.setLocalProperty
     */
    private static void updateLocalConfiguration(JavaSparkContext sparkContext, Configuration configuration) {
        /*
         * While we could enumerate over the entire SparkConfiguration and copy into the Thread
         * Local properties of the Spark Context this could cause adverse effects with future
         * versions of Spark. Since the api for setting multiple local properties at once is
         * restricted as private, we will only set those properties we know can effect SparkGraphComputer
         * Execution rather than applying the entire configuration.
         */

        String[] validPropertyNames = {
                "spark.job.description",
                "spark.jobGroup.id",
                "spark.job.interruptOnCancel",
                "spark.scheduler.pool"
        };

        for (String propertyName : validPropertyNames) {
            String propertyValue = configuration.get(propertyName);
            if (propertyValue != null) {
                LOGGER.info("Setting Thread Local SparkContext Property - "
                        + propertyName + " : " + propertyValue);
                sparkContext.setLocalProperty(propertyName, configuration.get(propertyName));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        FileConfiguration configuration = new PropertiesConfiguration(args[0]);
        new GraknSparkComputer(HadoopGraph.open(configuration))
                .program(VertexProgram.createVertexProgram(HadoopGraph.open(configuration), configuration))
                .submit().get();
    }
}
