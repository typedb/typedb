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
import java.util.UUID;

import ai.grakn.Keyspace;
import ai.grakn.concept.ConceptId;
import mjson.Json;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * TODO
 * @author Ganeshwara Herawan Hananda
 */
public class RocksDbQueue implements Queue {
    private final RocksDB db;
    public static final Path location = Paths.get("./queue.db");
    private final Options options = new Options()
            .setCreateIfMissing(true);

    public RocksDbQueue() {
        try {
            db = RocksDB.open(options, location.toAbsolutePath().toString());
        }
        catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    @Override
    public void insertAttribute(Attribute attribute) {
        byte[] id = attribute.conceptId().getValue().getBytes(UTF_8);
        byte[] attr = serialiseAttributeUtf8(attribute);
        WriteOptions syncWrite = new WriteOptions().setSync(true);
        try {
            db.put(syncWrite, id, attr);
        }
        catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    @Override
    public Attributes readAttributes(int min, int max, long maxWaitMs) {
        List<Attribute> result = new LinkedList<>();

        int count = 0;
        RocksIterator it = db.newIterator();
        it.seekToFirst();
        while (it.isValid() && count <= max) {
            String id = new String(it.key(), UTF_8);
            Attribute attr = deserialiseAttributeUtf8(it.value());
            result.add(attr);
            it.next();
            count++;
        }

        return new Attributes(result);
    }

    @Override
    public void ackAttributes(Attributes attributes) {
        WriteBatch acks = new WriteBatch();
        // set to false for better performance. at the moment we're setting it to true as the algorithm is untested and we prefer correctness over speed
        WriteOptions writeOptions = new WriteOptions().setSync(true);
        for (Attribute attr: attributes.attributes()) {
            acks.remove(attr.conceptId().getValue().getBytes(UTF_8));
        }
        try {
            db.write(writeOptions, acks);
        }
        catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    private byte[] serialiseAttributeUtf8(Attribute attribute) {
        Json json = Json.object(
                "attribute-keyspace", attribute.keyspace().getValue(),
                "attribute-value", attribute.value(),
                "attribute-concept-id", attribute.conceptId().getValue()
        );
        return json.toString().getBytes(UTF_8);
    }


    private Attribute deserialiseAttributeUtf8(byte[] attribute) {
        Json json = Json.read(new String(attribute, UTF_8));
        String keyspace = json.at("attribute-keyspace").asString();
        String value = json.at("attribute-value").asString();
        String conceptId = json.at("attribute-concept-id").asString();
        return Attribute.create(Keyspace.of(keyspace), value, ConceptId.of(conceptId));
    }
}