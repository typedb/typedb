/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;

use answer::Concept;
use storage::snapshot::ReadableSnapshot;

pub struct FetchStageExecutor<Snapshot: ReadableSnapshot> {
    ph: PhantomData<Snapshot>,
}

enum ConceptMapValue {
    Single(Concept<'static>),
    List(ConceptList),
    Map(ConceptMap),
}

struct ConceptMap {}

struct ConceptList {
    list: Vec<ConceptMapValue>,
}
