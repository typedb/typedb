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

* **CREATE** a new user:

`CREATE USER username WITH PASSWORD userpassword WITH ROLE admin`

* **UPDATE** an existing user's password

`UPDATE USER username WITH PASSWORD newpassword`

* **RETRIEVE** all of the present users:

`LIST USERS`

* **RETRIEVE** a user:

`GET USER username`

* **DELETE** an existing user:

`DELETE USER username`