package cis5550.kvs;

import static cis5550.webserver.Server.put;
import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.net.URLDecoder;

import cis5550.tools.Hasher;
import cis5550.tools.KeyEncoder;

public class Worker extends cis5550.generic.Worker {

    public static Map<String, Map<String, Row>> tables = new ConcurrentHashMap<>();
    public static String workDir;

    public static void main(String args[]) throws Exception {
        int portNum = Integer.valueOf(args[0]);
        port(portNum);
        workDir = args[1];
        String fileName = args[1] + "/id";

        File file = new File(fileName);
        // System.out.println("path: " + file.getAbsolutePath());
        String id = "";
        if (file.exists()) {
            id = new String(Files.readAllBytes(Paths.get(fileName)));
        } else {
            id = generate5RandomLetters();
            File dir = new File(args[1]);
            if (!dir.exists())
                dir.mkdir();
            // file.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            writer.write(id);
            writer.close();
        }

        startPingThread(args[0], args[2], id);
        get("/tables", (req, res) -> {
            res.header("content-type", "text/plain");
            String ans = "";
            for (String tableName : tables.keySet()) {
                ans += tableName + "\n";
            }

            File dir = new File(workDir);
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if (child.isDirectory()) {
                        ans += KeyEncoder.decode(child.getName()) + "\n";
                    }
                }
            } else {
                throw new Exception("Work directory doesn't exist");
            }

            return ans;
        });

        get("/", (req, res) -> {
            res.header("content-type", "text/html");
            String ans = "<html><table>";
            ans += "<tr>" + "<th>Table Name</th>" + "<th>Number of keys</th>" + "</tr>";
            for (String tableName : tables.keySet()) {
                Map<String, Row> table = tables.get(tableName);
                ans += "<tr>"
                        + "<td><a href=\"/view/" + tableName + "/\">" + tableName + "</a>" + "</td>"
                        + "<td>" + table.keySet().size() + "</td>"
                        + "</tr>";
            }

            File dir = new File(workDir);
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if (child.isDirectory()) {
                        ans += "<tr>"
                                + "<td><a href=\"/view/" + KeyEncoder.decode(child.getName()) + "/\">"
                                + KeyEncoder.decode(child.getName()) + "</a>" + "</td>"
                                + "<td>" + child.list().length + "</td>"
                                + "</tr>";
                    }
                }
            } else {
                throw new Exception("Work directory doesn't exist");
            }

            ans += "</table></html>";
            return ans;
        });

        get("/view/:tablename", (req, res) -> {
            res.header("content-type", "text/html");
            String fromRow = req.queryParams("fromRow");
            int remain1 = 10, remain2 = 10;
            String nextRow = null;
            Set<String> allColumNames = new HashSet<>();
            Map<String, Row> candidatesRows = new TreeMap<>((a, b) -> a.compareTo(b));

            if (!req.params("tablename").startsWith("pt-")) {
                Map<String, Row> table = tables.get(req.params("tablename"));
                if (req.params("tablename") == null || table == null) {
                    res.status(404, "Not Found");
                    return null;
                }

                for (String rowName : table.keySet()) {
                    Row row = table.get(rowName);
                    if (fromRow == null || fromRow.compareTo(row.key()) <= 0) {
                        candidatesRows.put(row.key(), row);
                    }
                }
            } else {

                File dir = new File(workDir);
                File[] matchingFiles = dir.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return req.params("tablename").equals(KeyEncoder.decode(name));
                    }
                });

                if (matchingFiles.length > 0) {
                    if (matchingFiles.length > 1) {
                        throw new Exception("Consists more than 1 matching tables");
                    }
                    for (File rowFile : matchingFiles[0].listFiles()) {
                        Row row = null;
                        try {
                            InputStream fileIn = new FileInputStream(rowFile);
                            row = Row.readFrom(fileIn);
                            fileIn.close();
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        if (fromRow == null || fromRow.compareTo(row.key()) <= 0) {
                            candidatesRows.put(row.key(), row);
                        }
                    }
                } else {
                    res.status(404, "Not Found");
                    return null;
                }
            }

            for (String rowKey : candidatesRows.keySet()) {
                if (remain1 == 0) {
                    break;
                }
                Row row = candidatesRows.get(rowKey);
                for (String colName : row.columns()) {
                    allColumNames.add(colName);
                }
                remain1--;
            }

            String ans = "<html><h2>" + req.params("tablename") + "</h2><table>";
            ans += "<tr>" + "<th>RowKey</th>";
            for (String colName : allColumNames) {
                ans += "<th>" + colName + "</th>";
            }
            ans += "</tr>";

            for (String rowKey : candidatesRows.keySet()) {
                if (remain2 == 0) {
                    nextRow = rowKey;
                    break;
                }
                Row row = candidatesRows.get(rowKey);
                ans += "<tr>";
                ans += "<td>" + row.key() + "</td>";
                for (String colName : allColumNames) {
                    if (row.columns().contains(colName)) {
                        ans += "<td>" + row.get(colName) + "</td>";
                    } else {
                        ans += "<td></td>";
                    }
                }
                ans += "</tr>";
                remain2--;
            }

            ans += "</table>";
            if (nextRow != null) {
                ans += "<a href=\"/view/" + req.params("tablename") + "?fromRow=" + nextRow + "\">"
                        + "Click to jump to next page</a>";
            }
            ans += "</html>";
            return ans;
        });

        put("/data/:table/:row/:col", (req, res) -> {
            String tableName = req.params("table");
            String rowName = req.params("row");
            String colName = req.params("col");

            if (tableName == null || rowName == null || colName == null) {
                res.status(400, "Bad Request");
                return null;
            }
            if (!tableName.startsWith("pt-")) {
                Row row = getRow(tableName, rowName);
                if (row == null) {
                    row = new Row(rowName);
                }
                row.put(colName, req.bodyAsBytes());
                putRow(tableName, row);
            } else {
                File dir = new File(workDir + "/" + KeyEncoder.encode(tableName));
                if (!dir.exists())
                    dir.mkdir();
                String fileNa = workDir + "/" + KeyEncoder.encode(tableName) + "/" + KeyEncoder.encode(rowName);
                File fi = new File(fileNa);
                Row row = new Row(rowName);
                row.put(colName, req.bodyAsBytes());
                if (!fi.exists()) {
                    FileOutputStream outputStream;
                    try {
                        outputStream = new FileOutputStream(fi);
                        outputStream.write(row.toByteArray());
                        outputStream.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    FileOutputStream outputStream;
                    try {
                        outputStream = new FileOutputStream(fi, true);
                        outputStream.write(row.contentToByteArray());
                        outputStream.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }

            return "OK";
        });

        get("/data/:table/:row/:col", (req, res) -> {
            String tableName = req.params("table");
            String rowName = req.params("row");
            String colName = req.params("col");

            if (tableName == null || rowName == null || colName == null) {
                res.status(400, "Bad Request");
                return null;
            }

            Row row = getRow(tableName, rowName);
            if (row == null) {
                res.status(404, "Not Found");
                return null;
            }
            byte[] data = row.getBytes(colName);
            if (data == null) {
                res.status(404, "Not Found");
                return null;
            }

            res.bodyAsBytes(data);
            return null;
        });

        get("/data/:table/:row", (req, res) -> {
            String tableName = req.params("table");
            String rowName = req.params("row");
            if (tableName == null || rowName == null) {
                res.status(400, "Bad Request");
                return null;
            }

            Row row = getRow(tableName, rowName);
            if (row == null) {
                res.status(404, "Not Found");
                return null;
            }

            res.bodyAsBytes(row.toByteArray());
            return null;
        });

        get("/data/:table", (req, res) -> {
            String tableName = req.params("table");
            if (tableName == null) {
                res.status(400, "Bad Request");
                return null;
            }

            res.header("content-type", "text/plain");

            String start = req.queryParams("startRow");
            String end = req.queryParams("endRowExclusive");

            byte[] lf = new byte[1];
            lf[0] = (byte) 10;

            if (!tableName.startsWith("pt-")) {
                Map<String, Row> table = tables.get(tableName);
                if (table == null) {
                    res.status(404, "Not Found");
                    return null;
                }

                for (String rowKey : table.keySet()) {
                    if ((start == null || start.compareTo(rowKey) <= 0) && (end == null || end.compareTo(rowKey) > 0)) {
                        Row row = table.get(rowKey);
                        res.write(row.toByteArray());
                        res.write(lf);

                    }
                }
                res.write(lf);
            } else {
                File dir = new File(workDir);
                File[] matchingFiles = dir.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return tableName.equals(KeyEncoder.decode(name));
                    }
                });

                if (matchingFiles.length > 0) {
                    if (matchingFiles.length > 1) {
                        throw new Exception("Consists more than 1 matching tables");
                    }
                    System.out.println("total file number: " + matchingFiles[0].listFiles().length);
                    int metFileNum = 0;
                    for (File rowFile : matchingFiles[0].listFiles()) {
                        Row row = null;
                        try {
                            InputStream fileIn = new FileInputStream(rowFile);
                            row = Row.readFrom(fileIn);
                            if ((start == null || start.compareTo(row.key()) <= 0)
                                    && (end == null || end.compareTo(row.key()) > 0)) {
                                res.write(row.toByteArray());
                                res.write(lf);
                                metFileNum++;
                                // if(row.key().equals("acjinmucsifehgqizmnkmecgpazoqmukrcecwers")) {
                                // System.out.println("met!!!");
                                // System.out.println("row to String: " + row.toString());
                                // System.out.println("row to ByteArr: ");
                                // for(byte b: row.toByteArray()) {
                                // System.out.print(b + " ");
                                // }
                                // }
                                // if(row.key().equals("acjqjklgpcdglgzcqevauolenedgikjcsqcobqfa")) {
                                // System.out.println("row to String: " + row.toString());
                                // System.out.println("row to ByteArr: ");
                                // for(byte b: row.toByteArray()) {
                                // System.out.print(b + " ");
                                // }
                                // }
                            }
                            fileIn.close();
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    System.out.println("met file number: " + metFileNum);
                    res.write(lf);
                } else {
                    res.status(404, "Not Found");
                    return null;
                }
            }

            return null;

        });

        get("/count/:table", (req, res) -> {
            String tableName = req.params("table");
            if (tableName == null) {
                res.status(400, "Bad Request");
                return null;
            }

            if (!tableName.startsWith("pt-")) {
                Map<String, Row> table = tables.get(tableName);
                if (table == null) {
                    res.status(404, "Not Found");
                    return null;
                }
                return "" + table.keySet().size();
            } else {
                File dir = new File(workDir);
                File[] matchingFiles = dir.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return tableName.equals(KeyEncoder.decode(name));
                    }
                });

                if (matchingFiles.length > 0) {
                    if (matchingFiles.length > 1) {
                        throw new Exception("Consists more than 1 matching tables");
                    }
                    return "" + matchingFiles[0].listFiles().length;
                } else {
                    res.status(404, "Not Found");
                    return null;
                }
            }
        });

        put("/rename/:table", (req, res) -> {
            String tableName = req.params("table");
            String newName = req.body();

            if (tableName == null || newName == null) {
                res.status(400, "Bad Request");
                return null;
            }

            if (tableName.startsWith("pt-") && !newName.startsWith("pt-")) {
                res.status(400, "Bad Request");
                return null;
            }

            if (!tableName.startsWith("pt-") && newName.startsWith("pt-")) {
                Map<String, Row> oldtable = tables.get(tableName);
                if (oldtable == null) {
                    res.status(404, "Not Found");
                    return null;
                }

                File dir = new File(workDir + "/" + KeyEncoder.encode(newName));
                if (dir.exists()) {
                    res.status(409, "Conflict");
                    return null;
                } else {
                    dir.mkdir();
                }

                for (String keyRow : oldtable.keySet()) {
                    Row row = oldtable.get(keyRow);
                    String fn = workDir + "/" + KeyEncoder.encode(newName) + "/" + KeyEncoder.encode(row.key());
                    File fi = new File(fn);
                    FileOutputStream outputStream;
                    try {
                        outputStream = new FileOutputStream(fi);
                        outputStream.write(row.toByteArray());
                        outputStream.close();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                tables.remove(tableName);
                return "OK";
            }

            if (!tableName.startsWith("pt-")) {
                Map<String, Row> table = tables.get(tableName);
                if (table == null) {
                    res.status(404, "Not Found");
                    return null;
                }

                if (tables.keySet().contains(newName)) {
                    res.status(409, "Conflict");
                    return null;
                }
                tables.put(newName, table);
                tables.remove(tableName);

            } else {
                File dir = new File(workDir);
                File[] matchingFiles = dir.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return tableName.equals(KeyEncoder.decode(name));
                    }
                });

                File[] conflictFiles = dir.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return newName.equals(KeyEncoder.decode(name));
                    }
                });

                if (conflictFiles.length > 0) {
                    res.status(409, "Conflict");
                    return null;
                }

                if (matchingFiles.length > 0) {
                    if (matchingFiles.length > 1) {
                        throw new Exception("Consists more than 1 matching tables");
                    }
                    File newFile = new File(KeyEncoder.encode(newName));
                    matchingFiles[0].renameTo(newFile);
                } else {
                    res.status(404, "Not Found");
                    return null;
                }
            }
            return "OK";
        });

        put("/delete/:table", (req, res) -> {
            String tableName = req.params("table");
            if (tableName == null) {
                res.status(400, "Bad Request");
                return null;
            }

            if (!tableName.startsWith("pt-")) {
                if (tables.containsKey(tableName)) {
                    tables.remove(tableName);
                } else {
                    res.status(404, "Not Found");
                    return null;
                }

            } else {
                File dir = new File(workDir);
                File[] matchingFiles = dir.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return tableName.equals(KeyEncoder.decode(name));
                    }
                });
                if (matchingFiles.length > 0) {
                    if (matchingFiles.length > 1) {
                        throw new Exception("Consists more than 1 matching tables");
                    }
                    for (File rowFile : matchingFiles[0].listFiles()) {
                        rowFile.delete();
                    }
                    matchingFiles[0].delete();
                } else {
                    res.status(404, "Not Found");
                    return null;
                }
            }
            return "OK";
        });
    }

    public static void putRow(String tableName, Row row) {
        if (tableName.startsWith("pt-")) {
            File dir = new File(workDir + "/" + KeyEncoder.encode(tableName));
            if (!dir.exists())
                dir.mkdir();
            String fileName = workDir + "/" + KeyEncoder.encode(tableName) + "/" + KeyEncoder.encode(row.key());
            File file = new File(fileName);
            FileOutputStream outputStream;
            try {
                outputStream = new FileOutputStream(file);
                outputStream.write(row.toByteArray());
                outputStream.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        } else {
            Map<String, Row> table = tables.getOrDefault(tableName, new ConcurrentHashMap<>());
            table.put(row.key(), row);
            tables.put(tableName, table);
        }
    }

    public static Row getRow(String tableName, String rowName) {
        if (tableName.startsWith("pt-")) {
            File dir = new File(workDir + "/" + KeyEncoder.encode(tableName));
            if (!dir.exists()) {
                return null;
            }
            String fileName = workDir + "/" + KeyEncoder.encode(tableName) + "/" + KeyEncoder.encode(rowName);
            File file = new File(fileName);
            if (!file.exists()) {
                return null;
            }

            Row row = null;
            try {
                InputStream fileIn = new FileInputStream(file);
                row = Row.readFrom(fileIn);
                fileIn.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

            // System.out.println(rowName);

            return row;
        } else {
            Map<String, Row> table = tables.get(tableName);
            if (table == null) {
                return null;
            }
            Row row = table.get(rowName);

            // System.out.println(rowName);

            return row;
        }
    }

}
