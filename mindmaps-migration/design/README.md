**Migrating Unstructured Data design *in-progress***

When migrating unstructured data, the user is required to write the ontology.

There are (*read: there will be*) more examples of data and syntax in the repos.


**GQLM**

The gqlm language is essentially a templating language for graql.

This is the design talked about on 14/2016:
```
insert
    $x isa person has name [name];
    (for i in [addresses].length){
        $%i isa address;
        $%i has street [addresses[%i].street];
        $%i has number [addresses[%i].houseNumber];
        ($x, $[i]) isa resides;
    }
```

After looking into more templating languages, here are some other options:

Similar to mustache/handlebars:
```
insert
    $x isa person;
    {{each addresses}}
        ${{@index}} isa address;
        ${{@index}} has street {{this.street}};
        ${{@index}} has number {{this.houseNumber}};
        ($x, ${{@index}}) isa resides;
    {{end each}}
```

Similar to google closure:
```
{foreach %address in %addresses}
    $index(%address) isa address;
    $index(%address) has street %address.street
    $index(%address) has number %address.houseNumber
    ($x, $index(%address)  isa resides;
{/foreach}

```

Personally, I prefer the latter two (in particular the middle one) because it looks less like programming.
They are all somewhat similar, so we could also create a combination of the above.

**Json Migration**


Given the json file:
```
{
    "name":"Alexandra",
    "addresses": [
        {
            "street":"Collins Ave.",
            "houseNumber": 8855
        },
        {
            "street":"Hornsey St.",
            "houseNumber":8
        }
    ]
}
```

When flattened, the above json looks like:
```
{
    "name": "Alexandra",
    "addresses[0].street": "Collins Ave.",
    "addresses[0].houseNumber": 8855,
    "addresses[1].street": "Hornsey St.",
    "addresses[1].houseNumber": 8
}

```

The user would have to define two things, the Mindmaps schema and the gqlm (Graql-migration) file:

schema:
```
insert
    person isa entity-type,
        has-resource name datatype string;
    address isa entity-type,
        has-resource street datatype string,
        has-resource number datatype long;

    resides isa relation-type, has-role person-residing, has-role place-residing;
    person-residing isa role-type;
    place-residing isa role-type;

    person plays-role person-residing;
    address plays-role place-residing;
```
gqlm:
```
insert
    $x isa person has name [name];
    (for %i in [addresses].length){
        $%i isa address;
        $%i has street [addresses[%i].street];
        $%i has number [addresses[%i].houseNumber];
        ($x, $[i]) isa resides;
    }
```

Executing the gqlm on the flattened json would result in:
```
insert
	$x isa person;
	$y0 isa address;
	$y0 has street [addresses.0.street];
	$y0 has number [addresses.0.houseNumber];
	($x, $y0) isa resides;
	$y1 isa address;
	$y1 has street [addresses.1.street];
	$y1 has number [addresses.1.houseNumber];
	($x, $y1) isa resides;
```

The above, along with a java Map of the flattened JSON, would be sent to parametrized graql to be executed and inserted as:
```

```

This would be executed for each JSON file


**CSV migration**

the csv "file" I will be referencing in the following commands:
```
address,icij_id,valid_until,country_codes,countries
8855 Collins Ave,6991059DFFB057DF310B9BF31CC4A0E6,2015,SGP,Singapore,
```

The user would define the schmea and the gqlm file:

schema:
```
insert address isa entity-type
    id icij_id
    has-resource valid_util datatype int,
    has-resource country_codes datatype string,
    has-resource country datatype string;
```

gqlm
```
insert $x isa address,
    has icij_id [icij_id],
    has valid_until [valid_until],
    has country_codes [countries_codes],
    has countries [countries];
```

This also allows us to migrate multiple entities, and even relationships between them, in one file:

schema:
```
    insert
        address isa entity-type,
            has-resource icij_id,
            has-resource valid_until;
        country isa entity-type,
            has-resource country-code;
        address-in-country isa relation-type,
            has-role country-of-address,
            has-role address-with-country;
        country plays-role country-of-address,
        address plays-role address-with-country;

```

gqlm:
```
    insert
        $x isa address,
            has icij_id [icij_id]
            has valid_until [valid_until]
        $y isa country,
            value [country]
            has country-code [country_codes]
        (address-with-country $x, country-of-address $y) isa address-in-country
```

And we would be able to migrate more complex files
(although this is messy, and please imagine "node_1" and "node_2" has been previously inserted):

schmea:
```
	node_1,rel_type,node_2
	11000001,intermediary of,10208879
	11000001,associated to,10198662
	11000001,eats,10159927
```

gqlm:
```
    match
        [node_1] isa $type-of-node-1,
        [node_2] isa $type-of-node-2
    insert
        // data
        ([rel_type]-role-1 [node_1], [rel_type]-role-2 [node_2]) isa [rel_type];

        // schema
        [rel_type] isa relation-type, has-role "[rel_type]-role-1", has-role [rel_type]-role-2;
        [rel_type]-role-1 isa role-type;
        [rel_type]-role-2 isa role-type;
        $type-of-node-1 plays-role [rel_type]-role-1,
        $type-of-node-2 plays-role [rel_type]-role-2;
```