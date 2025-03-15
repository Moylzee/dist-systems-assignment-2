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
        Integer numFiles = args.length;
        Integer numLines = 0;
        Integer numWords = 0;
        try {
            long fileReadStart = System.currentTimeMillis();

            // loop through the args (files) and reads them into the input map
            for (String file : args) {
                String content = Common.readFile(file);
                input.put(file, content);
            }
            long fileReadEnd = System.currentTimeMillis();
            timings.add("File Read Time: " + (fileReadEnd - fileReadStart) + "ms");
        } catch (IOException e) {
            System.err.println("Error reading files: " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

        // count the number of lines and words in the input
        for (Map.Entry<String, String> entry : input.entrySet()) {
            numLines += entry.getValue().split("\n").length;
            numWords += entry.getValue().split(" ").length;
        }

        timings.add("Number of Files: " + numFiles);
        timings.add("Total Number of Lines: " + numLines);
        timings.add("Total Number of Words: " + numWords);

        // APPROACH #1: Brute Force
        String bruteForceTime = BruteForce.BruteForceMethod(input);
        timings.add(bruteForceTime);

        // // APPROACH #2: MapReduce
        String[] mapReduceTime = MapReduce.MapReduceMethod(input);
        for (String time : mapReduceTime) {
            System.out.println(time);
            timings.add(time);
        }

        // APPROACH #3: Distributed MapReduce
        String[] distributedMapReduceTime = DistributedMapReduce.DistributedMapReduceMethod(input, 0, 0);
        for (String time : distributedMapReduceTime) {
            System.out.println(time);
            timings.add(time);
        }

        // Uncomment the below code to run the tests for the distributed map and reduce phases
        // testDistributedMapPhase(input);
        // testDistributedReducePhase(input);

        long totalEndTime = System.currentTimeMillis();
        timings.add("Total Time: " + (totalEndTime - totalStartTime) + "ms");
        Common.writeTimesToFile(timings);

    }

    private static void testDistributedMapPhase(Map<String, String> input) {
        Map<String, List<String>> mapTimes = new HashMap<>();
        for (int i = 1_000; i <= 10_000; i += 1000) {
            List<String> mapTimesList = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                String[] distributedMapReduceTime = DistributedMapReduce.DistributedMapReduceMethod(input, i, 0);
                mapTimesList.add(distributedMapReduceTime[0]);
            }
            mapTimes.put("Chunk Size per Thread: " + i, mapTimesList);
        }

        // Calculate the mean for each number of map threads
        for (Map.Entry<String, List<String>> entry : mapTimes.entrySet()) {
            double sum = 0;
            for (String time : entry.getValue()) {
                sum += Double.parseDouble(time.split(" ")[3].replace("ms", ""));
            }
            double mean = sum / entry.getValue().size();
            System.out.println(entry.getKey() + " Mean: " + mean + "ms");
        }

    }

    private static void testDistributedReducePhase(Map<String, String> input) {
        Map<String, List<String>> mapTimes = new HashMap<>();
        for (int i = 100; i <= 1_000; i += 100) {
            List<String> mapTimesList = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                String[] distributedMapReduceTime = DistributedMapReduce.DistributedMapReduceMethod(input, 0, i);
                mapTimesList.add(distributedMapReduceTime[2]);
            }
            mapTimes.put("Batch Size per Reduce Thread: " + i, mapTimesList);
        }

        // Calculate the mean for each number of map threads
        for (Map.Entry<String, List<String>> entry : mapTimes.entrySet()) {
            double sum = 0;
            for (String time : entry.getValue()) {
                sum += Double.parseDouble(time.split(" ")[3].replace("ms", ""));
            }
            double mean = sum / entry.getValue().size();
            System.out.println(entry.getKey() + " Mean: " + mean + "ms");
        }

    }

}
