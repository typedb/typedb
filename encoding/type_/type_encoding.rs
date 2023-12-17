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
 *
 */


pub(crate) mod concept {
    use wal::SequenceNumber;

    const ID_2_SIZE: usize = 2;

    struct TypeIIDSequenced {
        iid: TypeIID,
        sequence_number: SequenceNumber,
    }

    pub(crate) struct TypeIID {
        prefix: u8,
        id: TypeID,
    }

    pub(crate) struct TypeID {
        bytes: [u8; ID_2_SIZE],
    }
}

mod connection {

}