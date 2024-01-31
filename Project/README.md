# Compile & Launch Instructions (Windows)
All instructions run under the /project folder.

## (0) Compile webServer
javac -d classes --source-path src src/cis5550/webserver/Server.java

## (1) Compie & Launch the KVS Coordinator
javac  -d classes --source-path src src/cis5550/kvs/Coordinator.java
java -cp "classes" cis5550.kvs.Coordinator 8000

## (2) Compile & Launch the KVS Worker(s)
1. Open 3 workers named worker1, worker2, and worker3
javac -cp lib/json-simple-1.1.1.jar -d classes --source-path src src/cis5550/kvs/Worker.java
java -cp "lib/json-simple-1.1.1.jar;classes" cis5550.kvs.Worker 8001 worker1 localhost:8000

## (3) Compile & Launch Flame Coordinator
javac  -d classes --source-path src src/cis5550/flame/Coordinator.java
java -cp "classes" cis5550.flame.Coordinator 9000 localhost:8000

## (4) Compile & Launch Flame Worker(s)
javac -d classes --source-path src src/cis5550/flame/Worker.java
java -cp "classes" cis5550.flame.Worker 9001 localhost:9000

## (5) Compile & Execute the Indexer Job
The indexer reads its input from a KVS table called pt-crawl. Make sure a 'pt-crawl' table exists under a certain KVS worker. And then run the following:

javac -d classes --source-path src src/cis5550/flame/FlameSubmit.java\
javac -cp "classes" -d classes --source-path src src/cis5550/jobs/Indexer.java\
jar --create --file indexer.jar classes/cis5550/jobs/Indexer.class\
java -cp "classes" cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer

## (6) Compile & Execute the PageRank Job
The PageRank reads its input from a KVS table called pt-crawl as the Indexer. Make sure a 'pt-crawl' table exists under a certain KVS worker. And then run the following:

javac -cp "classes" -d classes --source-path src src/cis5550/jobs/PageRank.java\
jar --create --file pageRank.jar classes/cis5550/jobs/PageRank.class\
java -cp "classes" cis5550.flame.FlameSubmit localhost:9000 pageRank.jar cis5550.jobs.PageRank 0.01 80

## (7) Launch the Backend
javac -cp "classes" -d classes --source-path src src/cis5550/backend/Backend.java\
java -cp "classes" cis5550.backend.Backend 7000 localhost:8000 300000

## (8) Test the query
javac -cp "classes" -d classes --source-path src src/cis5550/test/QueryTest.java\
java -cp "classes" cis5550.test.QueryTest search

## (9) Test Frontend (macos)
Steps for starting the frontend server:
1. Compile the frontend server:
javac -d classes --source-path src src/cis5550/test/FrontendServerTest.java
2. Run the frontend server:
java -cp classes cis5550.test.FrontendServerTest 8010 7000
3. Note that the frontend server runs on port 8010, feel free to change if the port is occupied on your computer. Open port 8010 with "http://localhost:8010/SearchPage.html" on your browser.
4. This is just for starting the frontend server, you need to also start the backend server for the commissioning.
5. (very important)Since we are sending request from the browser to the frontend server and then send request from the frontend server to the backend server, the backend port in FrontendServer.java should also be changed according to your backend port.


