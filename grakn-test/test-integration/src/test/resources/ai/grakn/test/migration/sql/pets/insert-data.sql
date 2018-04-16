---
-- #%L
-- test-integration
-- %%
-- Copyright (C) 2016 - 2018 Grakn Labs Ltd
-- %%
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
-- 
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
-- 
-- You should have received a copy of the GNU Affero General Public License
-- along with this program.  If not, see <http://www.gnu.org/licenses/>.
-- #L%
---
INSERT INTO pet VALUES ('Puffball','Diane','hamster','f','1999-03-30',NULL);
INSERT INTO pet VALUES ('Fluffy', 'Harold', 'cat', 'f', '1993-02-04', NULL);
INSERT INTO pet VALUES ('Claws', 'Gwen', 'cat', 'm', '1994-03-17', NULL);
INSERT INTO pet VALUES ('Buffy', 'Harold', 'dog', 'f', '1989-05-13', NULL);
INSERT INTO pet VALUES ('Fang', 'Benny', 'dog', 'm', '1990-08-27', NULL);
INSERT INTO pet VALUES ('Bowser', 'Diane', 'dog', 'm', '1979-08-31', '1995-07-29');
INSERT INTO pet VALUES ('Chirpy', 'Gwen', 'bird', 'f', '1998-09-11', NULL);
INSERT INTO pet VALUES ('Whistler', 'Gwen', 'bird', 'N', '1997-12-09', NULL);
INSERT INTO pet VALUES ('Slim', 'Benny', 'snake', 'm', '1996-04-29', NULL);

INSERT INTO event VALUES ('Bowser', '1991-10-12', 'kennel', NULL);
INSERT INTO event VALUES ('Fang', '1991-10-12', 'kennel', NULL);
INSERT INTO event VALUES ('Fluffy', '1995-05-15', 'litter', '4 kittens, 3 female, 1 male');
INSERT INTO event VALUES ('Buffy', '1993-06-23', 'litter', '5 puppies, 2 female, 3 male');
INSERT INTO event VALUES ('Buffy', '1994-06-19', 'litter', '3 puppies, 3 female');
INSERT INTO event VALUES ('Chirpy', '1999-03-21', 'vet', 'needed beak straightened');
INSERT INTO event VALUES ('Slim', '1997-08-03', 'vet', 'broken rib');
INSERT INTO event VALUES ('Fang', '1998-08-28', 'birthday', 'Gave him a new chew toy');
INSERT INTO event VALUES ('Claws', '1998-03-17', 'birthday', 'Gave him a new flea collar');
INSERT INTO event VALUES ('Whistler', '1998-12-09', 'birthday', 'First birthday');
