/*
 * Copyright (C) 2023 Vaticle
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

/*
This file should comprise a set of low-level tests relating to MVCC.

1. We should be able to open a new snapshot (check open sequence number) and commit it. The next sequence number should have advanced by one (ie. watermark).

2. We should be able to open a previous snapshot, and doing reads on this version should ignore all newer versions

3. We should be able to open a previous snapshot in write, and the commit should fail if a subsequent commit has a conflicting operation
   Note: edge case is when the commit records have been purged from memory. Isolation manager should be able to retrieve them again from the Durability service on demand.
   Note: let's write this test first, and leave it red. This will induce a refactor of the Timeline - which is the next task.


4. We should be able to configure that a cleanup of old versionsn of key is run after T has elapsed that deletes old versions of data we no longer want to retain
   We want the time keeping component to be received from the durability service, which should be able to tell us the last sequence number
   before a specific time (it should be able to binary search its WAL log files and check the dates on them to find this information).
   After cleanup is run, we should get a good error if a version that is too-old is opened.
   After cleanup is run, if we iterate directly on the storage layer, we should be able to confirm the keys are actually not present anymore (Rocks may defer the disk delete till compaction, but to us they are "gone").

 */

// Moved to storage.rs
