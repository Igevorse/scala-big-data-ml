# About

The goal of this project is to implement big data processing using [Lambda-architecture](https://en.wikipedia.org/wiki/Lambda_architecture) in Scala using Spark and Scalatra.


# Build and run

Install Spark and Cassandra, run Cassandra by executing `sudo service cassandra start`.

Then execute: 
```bash
git clone https://github.com/Igevorse/scala-big-data-ml
cd scala-big-data-ml
sbt run
```

Streaming starts working after this moment!

In order to see real-time Twitter messages with a fancy modern interface, open [http://localhost:8080](http://localhost:8080) in your browser. 
If you want to retrieve first N records from the database, open http://localhost:8080/database/N, for example, [http://localhost:8080/database/10](http://localhost:8080/database/10).
