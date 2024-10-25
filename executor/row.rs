/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Cow, fmt, ops::Deref, slice, vec};

use answer::variable_value::VariableValue;
use compiler::VariablePosition;
use lending_iterator::higher_order::Hkt;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Row<'a> {
    row: &'a mut [VariableValue<'static>],
    multiplicity: &'a mut u64,
}

impl<'a> Row<'a> {
    // TODO: pub(crate)
    pub fn new(row: &'a mut [VariableValue<'static>], multiplicity: &'a mut u64) -> Self {
        Self { row, multiplicity }
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
    }

    pub(crate) fn copy_from(&mut self, row: &[VariableValue<'static>], multiplicity: u64) {
        debug_assert!(self.len() == row.len());
        self.row.clone_from_slice(row);
        *self.multiplicity = multiplicity;
    }

    pub(crate) fn get_multiplicity(&self) -> u64 {
        *self.multiplicity
    }

    pub(crate) fn set_multiplicity(&mut self, multiplicity: u64) {
        *self.multiplicity = multiplicity;
    }
}

impl<'a> fmt::Display for Row<'a> {
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
}

impl Hkt for MaybeOwnedRow<'static> {
    type HktSelf<'a> = MaybeOwnedRow<'a>;
}

impl<'a> MaybeOwnedRow<'a> {
    pub(crate) fn new_borrowed(row: &'a [VariableValue<'static>], multiplicity: &'a u64) -> Self {
        Self { row: Cow::Borrowed(row), multiplicity: Cow::Borrowed(multiplicity) }
    }

    pub(crate) fn new_from_row(row: &'a Row<'a>) -> Self {
        Self::new_borrowed(row.row, row.multiplicity)
    }

    // TODO: pub(crate)
    pub fn empty() -> Self {
        Self { row: Cow::Owned(Vec::new()), multiplicity: Cow::Owned(1) }
    }

    // TODO: pub(crate)
    pub fn new_owned(row: Vec<VariableValue<'static>>, multiplicity: u64) -> Self {
        Self { row: Cow::Owned(row), multiplicity: Cow::Owned(multiplicity) }
    }

    pub fn get(&self, position: VariablePosition) -> &VariableValue<'_> {
        &self.row[position.as_usize()]
    }

    pub fn multiplicity(&self) -> u64 {
        *self.multiplicity
    }

    pub fn row(&self) -> &[VariableValue<'static>] {
        self.row.as_ref()
    }

    pub fn into_owned(self) -> MaybeOwnedRow<'static> {
        let (row_vec, multiplicity) = self.into_owned_parts();
        MaybeOwnedRow { row: Cow::Owned(row_vec), multiplicity: Cow::Owned(multiplicity) }
    }

    pub fn as_reference(&self) -> MaybeOwnedRow<'_> {
        MaybeOwnedRow { row: Cow::Borrowed(self.row.as_ref()), multiplicity: Cow::Borrowed(self.multiplicity.as_ref()) }
    }

    pub fn into_owned_parts(self) -> (Vec<VariableValue<'static>>, u64) {
        (self.row.into_owned(), self.multiplicity.into_owned())
    }
}

impl<'a> Deref for MaybeOwnedRow<'a> {
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

impl<'a> fmt::Display for MaybeOwnedRow<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} x [  ", self.multiplicity)?;
        for value in &*self.row {
            write!(f, "{value}  ")?
        }
        write!(f, "]")?;
        Ok(())
    }
}
