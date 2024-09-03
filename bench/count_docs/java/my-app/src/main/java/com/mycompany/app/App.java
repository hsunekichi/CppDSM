package com.mycompany.app;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;
import org.redisson.api.RAtomicLong;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.BatchOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/*
public class App 
{
public static List<String> loadDocs(String directoryPath) throws IOException 
{
    final File folder = new File(directoryPath);
    List<String> docs = new ArrayList<String>();

    for (final File fileEntry : folder.listFiles()) {
        if (!fileEntry.isDirectory()) {
            docs.add(Files.readString(fileEntry.toPath()));
        }
    }

    return docs;
}

public static int countWords(String str) {
    return str.split("\\s+").length;
}

public static void wordsInCollection(RMap<String, String> collection, RAtomicLong count, int start, int end) {
    int size = collection.size();
    long localCount = 0;

    for (int i = start; i < end && i < size; i++) {
        localCount += countWords(collection.get(String.valueOf(i)));
    }

    count.addAndGet(localCount);
}

public static void main(String[] args) throws IOException {

    //String directoryPath = args[0];
    String directoryPath = "/home/hsunekichi/Escritorio/zaguan";

    RedissonClient redisson = Redisson.create();
    RMap<String, String> docsMap = redisson.getMap("docsMap");

    long startLoad = System.currentTimeMillis();

    List<String> docs = loadDocs(directoryPath);

    RBatch batch = redisson.createBatch(BatchOptions.defaults());

    for (int i = 0; i < docs.size(); ++i) {
        batch.getMap("docsMap").fastPutAsync(String.valueOf(i), docs.get(i));
    }

    BatchResult<?> result = batch.execute();


    long startComp = System.currentTimeMillis();

    //Redisson atomic int
    RAtomicLong count = redisson.getAtomicLong("count");

    wordsInCollection(docsMap, count, 0, docs.size());

    long end = System.currentTimeMillis();

    System.out.println("Number of docs: " + docs.size());
    System.out.println("Number of words: " + count.get() / 1000 + "k");
    System.out.println();
    System.out.println("Load time: " + (startComp - startLoad) + "ms");
    System.out.println("Computation time: " + (end - startComp) + "ms");
    System.out.println("Total time: " + (end - startLoad) + "ms");

    redisson.shutdown();
}
}
*/

public class App 
{
public static List<String> loadDocs(String directoryPath) throws IOException 
{
    final File folder = new File(directoryPath);
    List<String> docs = new ArrayList<String>();

    for (final File fileEntry : folder.listFiles()) {
        if (!fileEntry.isDirectory()) {
            docs.add(Files.readString(fileEntry.toPath()));
        }
    }

    return docs;
}

public static int countWords(String str) {
    return str.split("\\s+").length;
}

public static void wordsInCollection(Map<String, String> collection, AtomicLong count, int start, int end) {
    int size = collection.size();
    long localCount = 0;

    for (int i = start; i < end && i < size; i++) {
        localCount += countWords(collection.get(String.valueOf(i)));
    }

    count.addAndGet(localCount);
}

public static void main(String[] args) throws IOException {

    //String directoryPath = args[0];
    String directoryPath = "/home/hsunekichi/Escritorio/zaguan";

    // Create java map
    Map<String, String> docsMap = new HashMap<String, String>();


    long startLoad = System.currentTimeMillis();

    List<String> docs = loadDocs(directoryPath);

    for (int i = 0; i < docs.size(); ++i) {
        docsMap.put(String.valueOf(i), docs.get(i));
    }

    long startComp = System.currentTimeMillis();

    AtomicLong count = new AtomicLong(0);

    wordsInCollection(docsMap, count, 0, docs.size());

    long end = System.currentTimeMillis();

    System.out.println("Number of docs: " + docs.size());
    System.out.println("Number of words: " + count.get() / 1000 + "k");
    System.out.println();
    System.out.println("Load time: " + (startComp - startLoad) + "ms");
    System.out.println("Computation time: " + (end - startComp) + "ms");
    System.out.println("Total time: " + (end - startLoad) + "ms");
}
}