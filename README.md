reactive-source
===============

Open source framework to turn your database into a reactive stream of information.

Instead of you querying the database for new events, let the "database" (aka the reactive-source framework) notify you
that there is new or updated data available.

Currently supported database systems:

* **Postgres**
* **MySQL**

Upcoming database systems:

* **MongoDB**

Setup for Postgres
==================

There are two modes for the framework to run.

- Auto configuration mode (the framework will try to configure the database in your behalf)
- Manual configuration mode (it is your responsibility to configure the database)

Auto Configuration mode (Default)
===============

The user you are connecting to the database with, needs to have the following priviledges:

- **USAGE** (In order to set up a stored procedure)
- **TRIGGER** (In order to setup the needed triggers)

Manual Configuration Mode
======

In that case the user should configure the database with the needed procedure and triggers for the tables the user wants to monitor.

Under the resources directory there are some scripts provided to help the configuration of the database.

(More information will be added)

Checkout the code and run the tests
======

You will need to setup a local instance of a PostgreSQL database for the integration test to get executed.

Once you have installed a local instance of PostgreSQL, you will need to create the test database and user that are needed for the tests to run.

Checkout the code. Under the directory {CODE_DIR}/src/test/resources/scipts/psql you will find 2 sql. You only need the one named **create-db-and-user.sql**.

So the steps are:

1. Run the **create-db-and-user.sql** file with a user that has the privilege to create Databases and Roles.

    `
    psql -h localhost -U <USERNAME> -d <EXISTING_DB_NAME> -f create-db-and-user.sql
    `

2. Build the project. While on {CODE_DIR} run

    `
    mvn clean package
    `

Thats all!