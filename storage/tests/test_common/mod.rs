/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#[macro_export]
macro_rules! test_keyspace_set {
    {$($variant:ident => $id:literal : $name: literal),* $(,)?} => {
        #[derive(Clone, Copy)]
        enum TestKeyspaceSet { $($variant),* }
        impl KeyspaceSet for TestKeyspaceSet {
            fn iter() -> impl Iterator<Item = Self> { [$(Self::$variant),*].into_iter() }
            fn id(&self) -> KeyspaceId {
                match *self { $(Self::$variant => KeyspaceId($id)),* }
            }
            fn name(&self) -> &'static str {
                match *self { $(Self::$variant => $name),* }
            }
        }
    };
}