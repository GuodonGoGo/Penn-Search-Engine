package cis5550.test;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;

import cis5550.tools.HTTP;

public class QueryTest extends GenericTest {

  String id;

  QueryTest() {
    super();
    id = null;
  }

  void runSetup2() throws Exception {
    File f = new File("worker1");
    if (!f.exists())
      f.mkdir();

    File f2 = new File("worker1" + File.separator + "id");
    if (f2.exists())
      f2.delete();

    id = null;
  }

  void runSetup1() throws Exception {
    File f = new File("worker1");
    if (!f.exists())
      f.mkdir();

    PrintWriter idOut = new PrintWriter("worker1" + File.separator + "id");
    id = randomAlphaNum(5, 5);
    idOut.print(id);
    idOut.close();
  }

  void cleanup() throws Exception {
    File f2 = new File("worker1" + File.separator + "id");
    if (f2.exists())
      f2.delete();

    File f = new File("worker1");
    if (f.exists())
      f.delete();
  }

  void prompt() {
    /* Ask the user to confirm that the server is running */

    File f = new File("worker1");
    System.out.println("In two separate terminal windows, please run:");
    System.out.println("* java cis5550.kvs.Coordinator 8000");
    System.out.println("* java cis5550.kvs.Worker 8001 " + f.getAbsolutePath() + " localhost:8000");
    System.out.println(
        "and then hit Enter in this window to continue. If the Coordinator and/or the Worker are already running, please terminate them and restart the test suite!");
    (new Scanner(System.in)).nextLine();
  }

  void runTests(Set<String> tests) throws Exception {
    System.out.printf("\n%-10s%-40sResult\n", "Test", "Description");
    System.out.println("--------------------------------------------------------");

    if (tests.contains("search"))
      try {
        startTest("search", "Search for a term", 0);
        System.out.println("start search");
        Socket s = openSocket(7000);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/query?query=galaxy";
        String req = "GET " + cell;
        System.out.println("req: " + req);
        out.print(req + " HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        System.out.println("req sent");
        // HTTP.doRequest("GET",
        // "http://localhost:8000/query/?query=galaxy"+java.net.URLEncoder.encode(oldTableName,
        // "UTF-8")+"/", newTableName.getBytes()).body();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a " + r.statusCode + " response to our " + req
              + ", but we were expecting a 200 OK. Here is what was in the body:\n\n" + dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) {
        testFailed("An exception occurred: " + e, false);
        e.printStackTrace();
      }

    boolean stressRan = false;
    long reqPerSec = 0;
    long numFail = 0;

    if (numTestsFailed == 0)
      System.out.println("Looks like your solution passed all of the selected tests. Congratulations!");
    else
      System.out.println(numTestsFailed + " test(s) failed.");
    if (stressRan)
      System.out.println(
          "\nYour throughput in the stress test was " + reqPerSec + " requests/sec, with " + numFail + " failues");
    cleanup();
    closeOutputFile();
  }

  public static void main(String args[]) throws Exception {

    /*
     * Make a set of enabled tests. If no command-line arguments were specified, run
     * all tests.
     */

    Set<String> tests = new TreeSet<String>();
    boolean runSetup1 = true, runSetup2 = false, runTests = true, promptUser = true, outputToFile = false,
        exitUponFailure = true, cleanup = false;

    if ((args.length > 0) && (args[0].equals("auto1") || args[0].equals("auto2"))) {
      runSetup1 = false;
      runSetup2 = false;
      runTests = true;
      outputToFile = true;
      exitUponFailure = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && (args[0].equals("setup1") || args[0].equals("setup2"))) {
      runSetup1 = args[0].equals("setup1");
      runSetup2 = args[0].equals("setup2");
      runTests = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && (args[0].equals("cleanup1") || (args[0].equals("cleanup2")))) {
      runSetup1 = false;
      runSetup2 = false;
      runTests = false;
      promptUser = false;
      cleanup = true;
    } else if ((args.length > 0) && args[0].equals("version")) {
      System.out.println("HW4 worker autograder v1.3 (Sep 19, 2023)");
      System.exit(1);
    }

    if ((args.length == 0) || args[0].equals("all") || args[0].equals("auto1")) {
      tests.add("read-id");
      tests.add("put");
      tests.add("putget");
      tests.add("overwrite");
      tests.add("stress");
    } else if ((args.length > 0) && args[0].equals("auto2")) {
      tests.add("newid");
      tests.add("writeid");
    }

    for (int i = 0; i < args.length; i++)
      if (!args[i].equals("all") && !args[i].equals("auto1") && !args[i].equals("auto2") && !args[i].equals("setup1")
          && !args[i].equals("setup2") && !args[i].equals("cleanup1") && !args[i].equals("cleanup2"))
        tests.add(args[i]);

    QueryTest t = new QueryTest();
    t.setExitUponFailure(exitUponFailure);
    if (outputToFile)
      t.outputToFile();
    if (runSetup1)
      t.runSetup1();
    if (runSetup2)
      t.runSetup2();
    if (promptUser)
      t.prompt();
    if (runTests)
      t.runTests(tests);
    if (cleanup)
      t.cleanup();
  }
}
