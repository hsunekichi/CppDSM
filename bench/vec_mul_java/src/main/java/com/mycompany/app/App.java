package com.mycompany.app;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;
import org.redisson.api.RAtomicLong;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.BatchOptions;
import org.redisson.config.Config;
import org.redisson.api.LocalCachedMapOptions;

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
import java.util.Arrays;


public class App {

    /*
    static int vSize = 1000 * 1000 * 10;

    public static void initialize(RedissonClient redisson, Integer iMin, Integer iMax) {
        // Initialize the distributed data
        RMap<Integer, Integer> v1 = redisson.getMap("v1");
        RMap<Integer, Integer> v2 = redisson.getMap("v2");
        RMap<Integer, Integer> v3 = redisson.getMap("v3");

        for (Integer i = iMin; i < iMax; i++) {
            v1.set(i, i);
            v2.set(i, vSize - i);
        }
    }

    public static void multiply(RedissonClient redisson, Integer iMin, Integer iMax) {
        // Initialize the distributed data
        RMap<Integer, Integer> v1 = redisson.getMap("v1");
        RMap<Integer, Integer> v2 = redisson.getMap("v2");
        RMap<Integer, Integer> v3 = redisson.getMap("v3");

        // Multiply both vectors
        for (Integer i = iMin; i < iMax; i++) {
            v3.set(i, v1.get(i) * v2.get(i));
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // Initialize the Redisson client
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.1.50:6379");
        RedissonClient redisson = Redisson.create(config);

        // Initialize the distributed data
        RMap<Integer, Integer> v1 = redisson.getMap("v1");
        RMap<Integer, Integer> v2 = redisson.getMap("v2");
        RMap<Integer, Integer> v3 = redisson.getMap("v3");

        v1.clear();
        v2.clear();
        v3.clear();

        // Initialize both vectors
        v1.trySetCapacity(vSize);
        v2.trySetCapacity(vSize);
        v3.trySetCapacity(vSize);

        // Multiply both vectors

        // Create threads
        final int nThreads = 16;
        Thread[] threads = new Thread[nThreads];

        System.out.println("Launching " + nThreads + " threads");

        for (int i = 0; i < nThreads; i++) {
            final int finalI = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    initialize(redisson, finalI * vSize / nThreads, (finalI + 1) * vSize / nThreads);
                }
            });
        }

        // Join threads
        for (int i = 0; i < nThreads; i++) {
            threads[i].start();
        }

        for (int i = 0; i < nThreads; i++) {
            threads[i].join();
        }

        System.out.println("Initialization done");

        for (int i = 0; i < nThreads; i++) {
            final int finalI = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    multiply(redisson, finalI * vSize / nThreads, (finalI + 1) * vSize / nThreads);
                }
            });
        }

        // Join threads
        for (int i = 0; i < nThreads; i++) {
            threads[i].start();
        }

        for (int i = 0; i < nThreads; i++) {
            threads[i].join();
        }

        // Check result
        for (int i = 0; i < vSize; i++) {
            if (!v3.get(i).equals(i * (vSize - i))) {
                System.out.println("Error at index " + i);
                System.out.println("Expected: " + i * (vSize - i) + ", found: " + v3.get(i));
                System.exit(1);
            }
        }

        System.out.println("Test passed");

        redisson.shutdown();
        System.exit(0);
    }
}

*/


    public static void main(String[] args) 
    {
        Integer nVars = 100*1000;
        Boolean check_result = true;

        // Connect to Redis server. Make sure your Redis server is running.
        //RedissonClient redisson = Redisson.create();
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.1.49:6378");
        config.useSingleServer().setConnectionPoolSize(1000).setRetryAttempts(6); // Set an appropriate value based on your requirements
        config.useSingleServer().setConnectTimeout(5000)  // Set the connection timeout in milliseconds
                .setTimeout(3000);                               // Set the general operation timeout in milliseconds

        RedissonClient redisson = Redisson.create(config);

        System.out.println("Redisson client started");


        //RMap<Integer, Integer> v1 = redisson.getMap("v1");
        //RMap<Integer, Integer> v2 = redisson.getMap("v2");
        //RMap<Integer, Integer> v3 = redisson.getMap("v3");

        LocalCachedMapOptions opt = LocalCachedMapOptions.defaults();
        opt.storeCacheMiss(true);

        // Load vectors as cached maps
        RLocalCachedMap<Integer, Integer> v1 = redisson.getLocalCachedMap("v1", opt);
        RLocalCachedMap<Integer, Integer> v2 = redisson.getLocalCachedMap("v2", opt);
        RLocalCachedMap<Integer, Integer> v3 = redisson.getLocalCachedMap("v3", opt);
        
        //Map<String, String> v1 = new HashMap<>();
        //Map<String, String> v2 = new HashMap<>();
        //Map<String, String> v3 = new HashMap<>();

        v1.clear();
        v2.clear();
        v3.clear();

        for (int i = 0; i < nVars; i++) {
            v3.fastPutAsync(i, 0);
        }
        
        // Get time
        long startTime = System.nanoTime();

        // Initialize arrays with batch operations
        //RBatch batch = redisson.createBatch(BatchOptions.defaults());
        //for (int i = 0; i < nVars; i++) {
        //    batch.getMap("v1").putAsync(i, i);
        //    batch.getMap("v2").putAsync(i, nVars - i);
        //    batch.getMap("v3").putAsync(i, 0);
        //}
        //batch.execute();

        // Wait for the pipeline to complete
        

        
        for (Integer i = 0; i < nVars; i++) 
        {
            v1.fastPutAsync(i, i);
            Integer j = nVars - i;
            v2.fastPutAsync(i, j);
            v3.fastPutAsync(i, 0);
        }


        // Get time
        long initTime = System.nanoTime();

        // Writes using map writer
        

        v1.preloadCache();
        v2.preloadCache();


        // Sleep 3 seconds
        //try {
        //    Thread.sleep(3000);
        //} catch (InterruptedException e) {
        //    e.printStackTrace();
        //}

        long preloadTime = System.nanoTime();

        // Get time

        // Multiply the arrays element-wise
        RBatch batch2 = redisson.createBatch(BatchOptions.defaults());
        for (Integer i = 0; i < nVars; i++) 
        {
            Integer x = v1.get(i);
            Integer y = v2.get(i);
            Integer z = x * y;
            
            // Write the result using batch
            batch2.getMap("v3").putAsync(i, z);
        }

        batch2.execute();


        // Get time
        long multTime = System.nanoTime();

        v3.preloadCache();

        // Check the result
        if (check_result)
        {
            for (Integer i = 0; i < nVars; i++) {
                //if (Integer.parseInt(v3.get(i.toString())) != i * (nVars - i)) {
                if (v3.get(i) != i * (nVars - i)) {
                    System.out.println("Error at index " + i);
                    System.out.println("Expected: " + i * (nVars - i) + ", found: " + v3.get(i));
                    System.exit(1);
                }
            }
        }

        long endTime = System.nanoTime();

        // Get time

        System.out.println("****************************************");
        System.out.println("Test passed");

        System.out.println("Initialization time: " + (initTime - startTime) / 1000000 + " ms");
        System.out.println("Preload time: " + (preloadTime - initTime) / 1000000 + " ms");
        System.out.println("Multiplication time: " + (multTime - preloadTime) / 1000000 + " ms");
        System.out.println("Check time: " + (endTime - multTime) / 1000000 + " ms");
        System.out.println("Total time: " + (endTime - startTime) / 1000000 + " ms");
        System.out.println("****************************************");

        // Close the Redisson client
        redisson.shutdown();
    }

    private static void multiplyArrays(RedissonClient redisson, 
            RLocalCachedMap<Integer, Integer> v1, 
            RLocalCachedMap<Integer, Integer> v2, 
            RLocalCachedMap<Integer, Integer> v3) 
    {
        if (v1.size() != v2.size() || v1.size() != v3.size()) {
            throw new IllegalArgumentException("All input arrays must have the same size but have: "+ v1.size()+", "+ v2.size()+", "+ v3.size());
        }
        
        /*
        for (Integer i = 0; i < v1.size(); i++) 
        {
            String x = v1.get(i.toString());
            String y = v2.get(i.toString());
            Integer z = Integer.parseInt(x) * Integer.parseInt(y);
                    
            v3.put(i.toString(), z.toString());
        }
        */


        //RBatch batch = redisson.createBatch(BatchOptions.defaults());
        for (Integer i = 0; i < v1.size(); i++) 
        {
            //Integer x = v1.get(i);
            //Integer y = v2.get(i);
            //Integer z = x * y;
            
            // Write the result using batch
            //batch.getMap("v3").putAsync(i, z);
        }

        //batch.execute();
    }
}


/*
public static void main(String[] args) {

    Integer nVars = 1000*20;

    // Replace "yourMapName" with the actual name you want to use for your Redisson Map.
    Map<Integer, Integer> v1 = new HashMap<>();
    Map<Integer, Integer> v2 = new HashMap<>();
    Map<Integer, Integer> v3 = new HashMap<>();

    v1.clear();
    v2.clear();
    v3.clear();

    // Sample input arrays
    for (int i = 0; i < nVars; i++) {
        v1.put(i, i);
        v2.put(i, nVars - i);
        v3.put(i, 0);
    }
    

    // Multiply the arrays element-wise
    //multiplyArrays(v1, v2, v3);

    // Check the result
    //for (int i = 0; i < nVars; i++) {
    //    if (v3.get(i) != i * (nVars - i)) {
    //        System.out.println("Error at index " + i);
    //        System.out.println("Expected: " + i * (nVars - i) + ", found: " + v3.get(i));
    //        System.exit(1);
    //    }
    //}

    System.out.println("Test passed");
}

private static void multiplyArrays(Map<Integer, Integer> v1, 
        Map<Integer, Integer> v2, 
        Map<Integer, Integer> v3) 
{
    if (v1.size() != v2.size() || v1.size() != v3.size()) {
        throw new IllegalArgumentException("All input arrays must have the same size, have: "+ v1.size()+", "+ v2.size()+", "+ v3.size());
    }

    for (int i = 0; i < v1.size(); i++) {
        v3.put(i, v1.get(i) * v2.get(i));
    }
}
}
*/