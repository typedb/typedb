/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package ai.grakn.engine.attribute.uniqueness.queue;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import mjson.Json;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue.SerialisationUtils.deserialiseAttributeUtf8;
import static ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue.SerialisationUtils.deserializeStringUtf8;
import static ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue.SerialisationUtils.serialiseAttributeUtf8;
import static ai.grakn.engine.attribute.uniqueness.queue.RocksDbQueue.SerialisationUtils.serialiseStringUtf8;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implementation of a FIFO queue to support the attribute de-duplication in RocksDB.
 * It supports three operations: insert, read, and ack.
 *
 * The read and ack should be used together in order to provide fault-tolerance.
 * The attribute de-duplicator can read attributes from the queue and only ack after everything has been processed.
 * If the de-duplicator crashes during a deduplication, it can resume operation from the last ack-ed attribute.
 *
 * @author Ganeshwara Herawan Hananda
 */
public class RocksDbQueue implements AutoCloseable {
    private final RocksDB queueDb;

    /**
     * Instantiates the class and the queue data directory
     *
     * @param path where to persist the queue data
     */
    public RocksDbQueue(Path path) {
        try {
            Options options = new Options().setCreateIfMissing(true);
            queueDb = RocksDB.open(options, path.toAbsolutePath().toString());
        }
        catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    /**
     * insert a new attribute at the end of the queue.
     *
     * @param attribute the attribute to be inserted
     */
    public void insert(Attribute attribute) {
        WriteOptions syncWrite = new WriteOptions().setSync(true);
        try {
            queueDb.put(syncWrite, serialiseStringUtf8(attribute.conceptId().getValue()), serialiseAttributeUtf8(attribute));
            synchronized (this) { notifyAll(); }
        }
        catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    /**
     * Read at most N attributes from the beginning of the queue. Read everything if there are less than N attributes in the queue.
     * If the queue is empty, the method will block until the queue receives a new attribute.
     * The attributes won't be removed from the queue until you call {@link #ack(List<Attribute>)} on the returned attributes.
     *
     * @param limit the maximum number of items to be returned.
     * @return a list of {@link Attribute}
     * @throws InterruptedException
     * @see #ack(List<Attribute>)
     */
    public List<Attribute> read(int limit) throws InterruptedException {
        // blocks until the queue contains at least 1 element
        while (isQueueEmpty(queueDb)) {
            synchronized (this) { wait(); }
        }

        List<Attribute> result = new LinkedList<>();

        RocksIterator it = queueDb.newIterator();
        it.seekToFirst();
        int count = 0;
        while (it.isValid() && count < limit) {
            String id = deserializeStringUtf8(it.key());
            Attribute attr = deserialiseAttributeUtf8(it.value());
            System.out.println("id = " + id + ", attr = " + attr);
            result.add(attr);
            it.next();
            count++;
        }

        return result;
    }

    /**
     * Remove attributes from the queue.
     *
     * @param attributes the attributes which will be removed
     */
    public void ack(List<Attribute> attributes) {
        WriteBatch acks = new WriteBatch();
        // set to false for better performance. at the moment we're setting it to true as the algorithm is untested and we prefer correctness over speed
        WriteOptions writeOptions = new WriteOptions().setSync(true);
        for (Attribute attr: attributes) {
            acks.remove(serialiseStringUtf8(attr.conceptId().getValue()));
        }
        try {
            queueDb.write(writeOptions, acks);
        }
        catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    /**
     * Close the {@link RocksDbQueue} instance.
     */
    public void close() {
        queueDb.close();
    }

    /**
     * Check if the queue is empty.
     *
     * @param queueDb the queue to be checked
     * @return true if empty, false otherwise
     */
    private boolean isQueueEmpty(RocksDB queueDb) {
        RocksIterator it = queueDb.newIterator();
        it.seekToFirst();
        return !it.isValid();
    }

    /**
     * Serialisation helpers for the {@link RocksDbQueue}. Don't add any other serialisation methods that are not related to it.
     */
    static class SerialisationUtils {
        static byte[] serialiseAttributeUtf8(Attribute attribute) {
            Json json = Json.object(
                    "attribute-keyspace", attribute.keyspace().getValue(),
                    "attribute-value", attribute.value(),
                    "attribute-concept-id", attribute.conceptId().getValue()
            );
            return serialiseStringUtf8(json.toString());
        }

        static Attribute deserialiseAttributeUtf8(byte[] attribute) {
            Json json = Json.read(deserializeStringUtf8(attribute));
            String keyspace = json.at("attribute-keyspace").asString();
            String value = json.at("attribute-value").asString();
            String conceptId = json.at("attribute-concept-id").asString();
            return Attribute.create(Keyspace.of(keyspace), value, ConceptId.of(conceptId));
        }

        static String deserializeStringUtf8(byte[] bytes) {
            return new String(bytes, UTF_8);
        }

        static byte[] serialiseStringUtf8(String string) {
            return string.getBytes(UTF_8);
        }
    }
}