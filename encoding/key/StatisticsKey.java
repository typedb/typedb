/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.encoding.key;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.Bytes;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.Encoding.Metadata.Statistics;
import com.vaticle.typedb.core.encoding.iid.VertexIID;

import static com.vaticle.typedb.core.common.collection.ByteArray.join;

public class StatisticsKey implements Key {

    private static final Partition PARTITION = Partition.METADATA;

    private final ByteArray bytes;

    private StatisticsKey(ByteArray bytes) {
        assert bytes.hasPrefix(Encoding.Prefix.METADATA_STATISTICS.bytes());
        this.bytes = bytes;
    }

    @Override
    public ByteArray bytes() {
        return bytes;
    }

    @Override
    public Partition partition() {
        return PARTITION;
    }

    public static StatisticsKey vertexCount(VertexIID.Type typeIID) {
        return new StatisticsKey(join(
                Statistics.Prefix.VERTEX_COUNT.bytes(),
                typeIID.bytes()
        ));
    }

    public static StatisticsKey hasEdgeCount(VertexIID.Type thingTypeIID, VertexIID.Type attTypeIID) {
        return new StatisticsKey(join(
                Statistics.Prefix.HAS_EDGE_COUNT.bytes(),
                thingTypeIID.bytes(),
                attTypeIID.bytes()
        ));
    }

    public static StatisticsKey snapshot() {
        return new StatisticsKey(Statistics.Prefix.SNAPSHOT.bytes());
    }

    public static StatisticsKey txnCommitted(long txnID) {
        return new StatisticsKey(join(
                Statistics.Prefix.TXN_COMMITTED_ID.bytes(),
                ByteArray.encodeLong(txnID)
        ));
    }

    public static Prefix<StatisticsKey> txnCommittedPrefix() {
        return new Prefix<>(Statistics.Prefix.TXN_COMMITTED_ID.bytes(), Partition.METADATA, StatisticsKey::new);
    }

    public static class Miscountable extends StatisticsKey {

        private Miscountable(ByteArray bytes) {
            super(bytes);
            assert bytes.hasPrefix(Statistics.Prefix.MISCOUNTABLE.bytes());
        }

        public static Prefix<Miscountable> prefix() {
            return new Prefix<>(Statistics.Prefix.MISCOUNTABLE.bytes(), Partition.METADATA, Miscountable::new);
        }

        public static Miscountable attrOvercount(long txnID, VertexIID.Attribute<?> attIID) {
            return new Miscountable(join(
                    Statistics.Prefix.MISCOUNTABLE.bytes(),
                    ByteArray.encodeLong(txnID),
                    Statistics.Infix.ATTRIBUTE_OVERCOUNT.bytes(),
                    attIID.bytes()
            ));
        }

        public static Miscountable attrUndercount(long txnID, VertexIID.Attribute<?> attIID) {
            return new Miscountable(join(
                    Statistics.Prefix.MISCOUNTABLE.bytes(),
                    ByteArray.encodeLong(txnID),
                    Statistics.Infix.ATTRIBUTE_UNDERCOUNT.bytes(),
                    attIID.bytes()
            ));
        }

        public static Miscountable hasEdgeOvercount(long txnID, VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            return new Miscountable(join(
                    Statistics.Prefix.MISCOUNTABLE.bytes(),
                    ByteArray.encodeLong(txnID),
                    Statistics.Infix.HAS_EDGE_OVERCOUNT.bytes(),
                    thingIID.bytes(),
                    attIID.bytes()
            ));
        }

        public static Miscountable hasEdgeUndercount(long txnID, VertexIID.Thing thingIID, VertexIID.Attribute<?> attIID) {
            return new Miscountable(join(
                    Statistics.Prefix.MISCOUNTABLE.bytes(),
                    ByteArray.encodeLong(txnID),
                    Statistics.Infix.HAS_EDGE_UNDERCOUNT.bytes(),
                    thingIID.bytes(),
                    attIID.bytes()
            ));
        }

        public boolean isAttrOvertcount() {
            return infix() == Statistics.Infix.ATTRIBUTE_OVERCOUNT.key();
        }

        public boolean isAttrUndercount() {
            return infix() ==
                    Statistics.Infix.ATTRIBUTE_UNDERCOUNT.key();
        }

        public boolean isHasEdgeOvercount() {
            return infix() == Statistics.Infix.HAS_EDGE_OVERCOUNT.key();
        }

        public boolean isHasEdgeUndercount() {
            return infix() == Statistics.Infix.HAS_EDGE_UNDERCOUNT.key();
        }

        private byte infix() {
            return bytes().get(Statistics.Prefix.LENGTH + Bytes.LONG_SIZE);
        }

        public VertexIID.Attribute<?> getMiscountableAttribute() {
            assert isAttrOvertcount() || isAttrUndercount();
            return VertexIID.Attribute.extract(
                    this.bytes(),
                    Statistics.Prefix.LENGTH + Bytes.LONG_SIZE + Statistics.Infix.LENGTH
            );
        }

        public Pair<VertexIID.Thing, VertexIID.Attribute<?>> getMiscountableHasEdge() {
            assert isHasEdgeOvercount() || isHasEdgeUndercount();
            VertexIID.Thing owner = VertexIID.Thing.extract(
                    this.bytes(),
                    Statistics.Prefix.LENGTH + Bytes.LONG_SIZE + Statistics.Infix.LENGTH
            );
            return new Pair<>(
                    owner,
                    VertexIID.Attribute.extract(
                            this.bytes(),
                            Statistics.Prefix.LENGTH + Bytes.LONG_SIZE + Statistics.Infix.LENGTH + owner.bytes().length()
                    )
            );
        }
    }
}
