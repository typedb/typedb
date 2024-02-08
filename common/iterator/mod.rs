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

use std::borrow::Borrow;
use std::marker::PhantomData;

pub trait RefIterator<'a, Ref: 'a> {
    fn next(&'a mut self) -> Option<Ref>;

    fn map<MappedRef: 'a, F>(self, mapper: F) -> MapRefIterator<Ref, Self, F>
        where
            Self: Sized,
            F: FnMut(Ref) -> MappedRef {
        MapRefIterator::new(self, mapper)
    }
}

pub struct MapRefIterator<Ref, I, F> {
    ref_iterator: I,
    mapper: F,
    phantom_data: PhantomData<Ref>,
}

impl<'a, Ref: 'a, I: RefIterator<'a, Ref>, F, MappedRef: 'a> MapRefIterator<Ref, I, F>
    where
        F: FnMut(Ref) -> MappedRef {

    fn new(ref_iterator: I, mapper: F) -> Self {
        Self {
            ref_iterator: ref_iterator,
            mapper: mapper,
            phantom_data: PhantomData::default(),
        }
    }
}

impl<'a, Ref: 'a, I: RefIterator<'a, Ref>, F, MappedRef: 'a> RefIterator<'a, MappedRef> for MapRefIterator<Ref, I, F>
    where F: FnMut(Ref) -> MappedRef {

    fn next(&'a mut self) -> Option<MappedRef> {
        self.ref_iterator.next().map(|next| (self.mapper)(next))
    }
}
