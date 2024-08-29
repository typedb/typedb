/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::marker::PhantomData;
use std::sync::Arc;

use concept::thing::thing_manager::ThingManager;
use storage::snapshot::WritableSnapshot;

use crate::{
    pipeline::PipelineError,
    write::delete::DeleteExecutor,
};
use crate::pipeline::{StageAPI, StageIterator, WrittenRowsIterator};

pub struct DeleteStageExecutor<Snapshot: WritableSnapshot + 'static, PreviousStage: StageAPI<Snapshot>> {
    deleter: DeleteExecutor,
    previous: PreviousStage,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for DeleteStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: WritableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>, Arc<ThingManager>), PipelineError> {
        let (previous_iterator, mut snapshot, mut thing_manager) = self.previous.into_iterator()?;
        // accumulate once, then we will operate in-place
        let mut rows = previous_iterator.collect_owned();

        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so unwrap:
        let snapshot_ref = Arc::get_mut(&mut snapshot).unwrap();
        let thing_manager_ref = Arc::get_mut(&mut thing_manager).unwrap();
        for row in &mut rows {
            self.deleter.execute_delete(snapshot_ref, thing_manager_ref, &mut row.as_mut_ref())
                .map_err(|err| PipelineError::WriteError(err))?;
        }

        Ok((WrittenRowsIterator::new(rows), snapshot, thing_manager))
    }
}
