package grakn.core.traversal.producer;

import grakn.core.common.async.Producer;
import grakn.core.graph.vertex.Vertex;
import graql.lang.pattern.variable.Reference;

import java.util.Map;

public interface TraversalProducer extends Producer<Map<Reference, Vertex<?, ?>>> {}
