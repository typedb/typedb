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
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

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
 * TODO
 * @author Ganeshwara Herawan Hananda
 */
public class RocksDbQueue implements Queue {
    private final RocksDB queueDb;
    private AtomicLong queueSize = new AtomicLong(0L);

    public RocksDbQueue() {
        try {
            Options options = new Options().setCreateIfMissing(true);
            Path path = Paths.get("./queue");
            queueDb = RocksDB.open(options, path.toAbsolutePath().toString());
        }
        catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    //    @Override
    public void insertAttribute(Attribute attribute) {
        WriteOptions syncWrite = new WriteOptions().setSync(true);
        try {
            queueDb.put(syncWrite, serialiseStringUtf8(attribute.conceptId().getValue()), serialiseAttributeUtf8(attribute));
            queueSize.incrementAndGet();
            notifyAll();
        }
        catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    //    @Override
    public Attributes readAttributes(int limit) throws InterruptedException {
        // blocks until the queue contains at least 1 element
        while (queueSize.get() == 0) { wait(); }

        List<Attribute> result = new LinkedList<>();

        int count = 0;
        RocksIterator it = queueDb.newIterator();
        it.seekToFirst();
        while (it.isValid() && count < limit) {
            String id = deserializeStringUtf8(it.key());
            Attribute attr = deserialiseAttributeUtf8(it.value());
            System.out.println("id = " + id + ", attr = " + attr);
            result.add(attr);
            it.next();
            count++;
        }

        return new Attributes(result);
    }

    //    @Override
    public void ackAttributes(Attributes attributes) {
        WriteBatch acks = new WriteBatch();
        // set to false for better performance. at the moment we're setting it to true as the algorithm is untested and we prefer correctness over speed
        WriteOptions writeOptions = new WriteOptions().setSync(true);
        for (Attribute attr: attributes.attributes()) {
            acks.remove(serialiseStringUtf8(attr.conceptId().getValue()));
        }
        try {
            queueDb.write(writeOptions, acks);
            long decrementBy = -1L * attributes.attributes().size();
            queueSize.getAndAdd(decrementBy);
        }
        catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    public void close() {
        queueDb.close();
    }

    static class SerialisationUtils {
        public static byte[] serialiseAttributeUtf8(Attribute attribute) {
            Json json = Json.object(
                    "attribute-keyspace", attribute.keyspace().getValue(),
                    "attribute-value", attribute.value(),
                    "attribute-concept-id", attribute.conceptId().getValue()
            );
            return serialiseStringUtf8(json.toString());
        }

        public static Attribute deserialiseAttributeUtf8(byte[] attribute) {
            Json json = Json.read(deserializeStringUtf8(attribute));
            String keyspace = json.at("attribute-keyspace").asString();
            String value = json.at("attribute-value").asString();
            String conceptId = json.at("attribute-concept-id").asString();
            return Attribute.create(Keyspace.of(keyspace), value, ConceptId.of(conceptId));
        }

        public static String deserializeStringUtf8(byte[] bytes) {
            return new String(bytes, UTF_8);
        }

        public static byte[] serialiseStringUtf8(String string) {
            return string.getBytes(UTF_8);
        }
    }
}