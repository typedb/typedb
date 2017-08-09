DROP TABLE IF EXISTS pet;

CREATE TABLE pet
(
  name    VARCHAR(20),
  owner   VARCHAR(20),
  species VARCHAR(20),
  sex     CHAR(1),
  birth   DATE,
  death   DATE
);

DROP TABLE IF EXISTS event;

CREATE TABLE event
(
  name   VARCHAR(20),
  date   DATE,
  eventtype   VARCHAR(15),
  remark VARCHAR(255)
);

CREATE TABLE empty
(
  name   VARCHAR(20),
  date   DATE
)