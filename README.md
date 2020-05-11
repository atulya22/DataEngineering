### Data Engineering Projects
#### Relational Data Modeling with PostgreSQL
We set up an ETL script to populate a Postgres database consisting of facts and dimension tables using data obtained from a music streaming application. The dataset consists of metadata about songs and user activity logs, both of which are in JSON format. The tables are modeled using star schema.

[Relational Data Modeling with PostgreSQL](https://github.com/atulya22/DataEngineering/tree/master/RDMS_Data_Modelling)

#### NoSQL Data Modeling with Apache Casandra
We set up an ETL script to merge several smaller CSV files, consisting of user activity logs for a music streaming application, into a single file. Using Apache Cassandra as the database, we then create tables for the activity logs. The tables are modeled off of the queries requested by the clients. The tables make use of partition keys to help separate our data across different nodes and use clustering columns to provide order for the data as well as form a unique primary key. 

[NoSQL Data Modeling with Apache Casandra](https://github.com/atulya22/DataEngineering/tree/master/NoSQL_Data_Modelling)
