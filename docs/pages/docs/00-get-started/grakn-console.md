---
title: Grakn Console (KGMS only)
keywords: grakn, kgms
tags: [kgms]
summary: "
The Grakn Console is used to manage KGMS users."
sidebar: documentation_sidebar
permalink: /docs/get-started/grakn-console
folder: docs
---

The Grakn console executable is contained in the `bin` folder of the distribution and is shared with `server` and `cluster` functionalities.
Once the grakn server is running, to open grakn console, navigate to the `bin` directory and run:

```
./grakn console start
```

Once run, you will be prompted for user credentials. Provided you log in as user with `admin` privileges, Grakn console allows you to perform the following actions:

* create a new user:

`CREATE USER username WITH PASSWORD userpassword WITH ROLE admin`

* update an existing user's password

`UPDATE USER username WITH PASSWORD newpassword`

* retrieve all of the users present:

`LIST USERS`

* retrieve a user:

`GET USER username`

* delete an existing user:

`DELETE USER username`