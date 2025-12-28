/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{cmp::Ordering, mem, mem::transmute};

use lending_iterator::{LendingIterator, Seekable};
use resource::profile::StorageCounters;
use rocksdb::DBRawIterator;

use crate::snapshot::pool::PoolRecycleGuard;

type KeyValue<'a> = (&'a [u8], &'a [u8]);
