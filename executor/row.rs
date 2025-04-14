/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, fmt, ops::Deref, slice, vec};

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use ir::pattern::BranchID;
use lending_iterator::higher_order::Hkt;

use crate::Provenance;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Row<'a> {
    row: &'a mut [VariableValue<'static>],
    multiplicity: &'a mut u64,
    provenance: &'a mut Provenance,
}

impl<'a> Row<'a> {
    // TODO: pub(crate)
    pub fn new(
        row: &'a mut [VariableValue<'static>],
        multiplicity: &'a mut u64,
        provenance: &'a mut Provenance,
    ) -> Self {
        Self { row, multiplicity, provenance }
    }

    pub(crate) fn len(&self) -> usize {
        self.row.len()
    }

    pub fn get(&self, position: VariablePosition) -> &VariableValue<'static> {
        &self.row[position.as_usize()]
    }

    pub(crate) fn unset(&mut self, position: VariablePosition) {
        self.row[position.as_usize()] = VariableValue::Empty;
    }

    pub(crate) fn set(&mut self, position: VariablePosition, value: VariableValue<'static>) {
        debug_assert!(
            *self.get(position) == VariableValue::Empty || *self.get(position) == value,
            "{} != {}",
            self.get(position),
            value,
        );
        self.row[position.as_usize()] = value;
    }

    pub(crate) fn copy_from_row(&mut self, row: MaybeOwnedRow<'_>) {
        debug_assert!(self.len() >= row.len());
        self.row[0..row.len()].clone_from_slice(row.row());
        *self.multiplicity = row.multiplicity();
        *self.provenance = row.provenance()
    }

    pub(crate) fn copy_from(&mut self, row: &[VariableValue<'static>], multiplicity: u64, provenance: Provenance) {
        debug_assert!(self.len() == row.len());
        self.row.clone_from_slice(row);
        *self.multiplicity = multiplicity;
        *self.provenance = provenance
    }

    pub(crate) fn copy_mapped(
        &mut self,
        row: MaybeOwnedRow<'_>,
        mapping: impl Iterator<Item = (VariablePosition, VariablePosition)>,
    ) {
        for (src, dst) in mapping {
            if src.as_usize() < row.len() {
                self.set(dst, row.get(src).clone().into_owned());
            }
        }
        *self.multiplicity = *row.multiplicity;
        *self.provenance = *row.provenance
    }

    pub fn get_multiplicity(&self) -> u64 {
        *self.multiplicity
    }

    pub(crate) fn set_multiplicity(&mut self, multiplicity: u64) {
        *self.multiplicity = multiplicity;
    }

    pub(crate) fn get_provenance(&self) -> Provenance {
        *self.provenance
    }

    pub(crate) fn set_provenance(&mut self, provenance: Provenance) {
        *self.provenance = provenance;
    }

    pub(crate) fn set_branch_id_in_provenance(&mut self, branch_id: BranchID) {
        self.provenance.set_branch_id(branch_id)
    }
}

impl fmt::Display for Row<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} x [  ", self.multiplicity)?;
        for value in &*self.row {
            write!(f, "{value}  ")?
        }
        writeln!(f, "]")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct MaybeOwnedRow<'a> {
    row: Cow<'a, [VariableValue<'static>]>,
    multiplicity: Cow<'a, u64>,
    provenance: Cow<'a, Provenance>, // TODO: Review: are we not better off without the Cow?
}

impl Hkt for MaybeOwnedRow<'static> {
    type HktSelf<'a> = MaybeOwnedRow<'a>;
}

impl<'a> MaybeOwnedRow<'a> {
    pub(crate) fn new_borrowed(
        row: &'a [VariableValue<'static>],
        multiplicity: &'a u64,
        provenance: &'a Provenance,
    ) -> Self {
        Self {
            row: Cow::Borrowed(row),
            multiplicity: Cow::Borrowed(multiplicity),
            provenance: Cow::Borrowed(provenance),
        }
    }

    pub(crate) fn new_from_row(row: &'a Row<'a>) -> Self {
        Self::new_borrowed(row.row, row.multiplicity, row.provenance)
    }

    // TODO: pub(crate)
    pub fn empty() -> Self {
        Self { row: Cow::Owned(Vec::new()), multiplicity: Cow::Owned(1), provenance: Cow::Owned(Provenance(0)) }
    }

    // TODO: pub(crate)
    pub fn new_owned(row: Vec<VariableValue<'static>>, multiplicity: u64, provenance: Provenance) -> Self {
        Self { row: Cow::Owned(row), multiplicity: Cow::Owned(multiplicity), provenance: Cow::Owned(provenance) }
    }

    pub fn get(&self, position: VariablePosition) -> &VariableValue<'_> {
        &self.row[position.as_usize()]
    }

    pub fn multiplicity(&self) -> u64 {
        *self.multiplicity
    }

    pub(crate) fn provenance(&self) -> Provenance {
        *self.provenance
    }

    pub fn row(&self) -> &[VariableValue<'static>] {
        self.row.as_ref()
    }

    pub fn into_owned(self) -> MaybeOwnedRow<'static> {
        let (row_vec, multiplicity, provenance) = self.into_owned_parts();
        MaybeOwnedRow {
            row: Cow::Owned(row_vec),
            multiplicity: Cow::Owned(multiplicity),
            provenance: Cow::Owned(provenance),
        }
    }

    pub fn as_reference(&self) -> MaybeOwnedRow<'_> {
        MaybeOwnedRow {
            row: Cow::Borrowed(self.row.as_ref()),
            multiplicity: Cow::Borrowed(self.multiplicity.as_ref()),
            provenance: Cow::Borrowed(self.provenance.as_ref()),
        }
    }

    pub fn into_owned_parts(self) -> (Vec<VariableValue<'static>>, u64, Provenance) {
        (self.row.into_owned(), self.multiplicity.into_owned(), self.provenance.into_owned())
    }
}

impl Deref for MaybeOwnedRow<'_> {
    type Target = [VariableValue<'static>];

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

impl IntoIterator for MaybeOwnedRow<'static> {
    type Item = VariableValue<'static>;
    type IntoIter = vec::IntoIter<VariableValue<'static>>;
    fn into_iter(self) -> Self::IntoIter {
        #[allow(clippy::unnecessary_to_owned)]
        self.row.into_owned().into_iter()
    }
}

impl<'a, 'b: 'a> IntoIterator for &'a MaybeOwnedRow<'b> {
    type Item = &'a VariableValue<'b>;
    type IntoIter = slice::Iter<'a, VariableValue<'b>>;
    fn into_iter(self) -> Self::IntoIter {
        self.row.iter()
    }
}

impl fmt::Display for MaybeOwnedRow<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} x [  ", self.multiplicity)?;
        for value in &*self.row {
            write!(f, "{value}  ")?
        }
        write!(f, "]")?;
        Ok(())
    }
}
