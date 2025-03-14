package com.distsys.a2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.distsys.a2.approach.BruteForce;
import com.distsys.a2.approach.DistributedMapReduce;
import com.distsys.a2.approach.MapReduce;
import com.distsys.a2.utils.Common;

public class App {
    

    public static void main(String[] args) {

        long totalStartTime = System.currentTimeMillis();
        List<String> timings = new ArrayList<>();

        if (args.length < 3) {
            System.err.println("Usage: java MapReduceFiles.java file1.txt file2.txt file3.txt");
            return;
        }

        Map<String, String> input = new HashMap<>();
        try {
            long fileReadStart = System.currentTimeMillis();

            // loop through the args (files) and reads them into the input map
            for (String file : args) {
                input.put(file, Common.readFile(file));
            }
            long fileReadEnd = System.currentTimeMillis();
            timings.add("File Read Time: " + (fileReadEnd - fileReadStart) + "ms");
        } catch (IOException e) {
            System.err.println("Error reading files: " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

        // APPROACH #1: Brute Force
        String bruteForceTime = BruteForce.BruteForceMethod(input);
        timings.add(bruteForceTime);

        // APPROACH #2: MapReduce
        String[] mapReduceTime = MapReduce.MapReduceMethod(input);
        for (String time : mapReduceTime) {
            System.out.println(time);
            timings.add(time);
        }

        System.out.println("-------");

        // APPROACH #3: Distributed MapReduce
        String[] distributedMapReduceTime = DistributedMapReduce.DistributedMapReduceMethod(input);
        for (String time : distributedMapReduceTime) {
            System.out.println(time);
            timings.add(time);
        }

        long totalEndTime = System.currentTimeMillis();
        timings.add("Total Time: " + (totalEndTime - totalStartTime) + "ms");
        Common.writeTimesToFile(timings);

    }

}
