
CREATE TABLE city (
  id integer NOT NULL PRIMARY KEY,
  name text NOT NULL,
  countrycode character(3) NOT NULL,
  district text NOT NULL,
  population integer NOT NULL
);

CREATE TABLE country (
  code character(3) NOT NULL PRIMARY KEY,
  name text NOT NULL,
  continent text NOT NULL,
  region text NOT NULL,
  surfacearea real NOT NULL,
  indepyear smallint,
  population integer NOT NULL,
  lifeexpectancy real,
  gnp numeric(10,2),
  gnpold numeric(10,2),
  localname text NOT NULL,
  governmentform text NOT NULL,
  headofstate text,
  capital integer,
  code2 character(2) NOT NULL,
  CONSTRAINT country_continent_check CHECK ((((((((continent = 'Asia'::text) OR (continent = 'Europe'::text)) OR (continent = 'North America'::text)) OR (continent = 'Africa'::text)) OR (continent = 'Oceania'::text)) OR (continent = 'Antarctica'::text)) OR (continent = 'South America'::text)))
);

CREATE TABLE countrylanguage (
  countrycode character(3) NOT NULL,
  language varchar(255) NOT NULL,
  isofficial boolean NOT NULL,
  percentage real NOT NULL,
  PRIMARY KEY(countrycode, language)
);

--
-- Add foreign key constraints.
--

ALTER TABLE country ADD CONSTRAINT country_capital_fkey FOREIGN KEY (capital) REFERENCES city(id);

ALTER TABLE countrylanguage ADD CONSTRAINT countrylanguage_countrycode_fkey FOREIGN KEY (countrycode) REFERENCES country(code);