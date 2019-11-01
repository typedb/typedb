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

package grakn.core.graph.diskstorage.log.kcvs;


import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.log.kcvs.KCVSLog;
import grakn.core.graph.diskstorage.log.util.AbstractMessage;

import java.time.Instant;

/**
 * Implementation of {@link AbstractMessage} for {@link KCVSLog}.
 *
 */
public class KCVSMessage extends AbstractMessage {

    public KCVSMessage(StaticBuffer payload, Instant timestamp, String senderId) {
        super(payload, timestamp, senderId);
    }
}
