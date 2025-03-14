package com.distsys.a2.approach;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.distsys.a2.callback.MapCallback;
import com.distsys.a2.callback.ReduceCallback;
import com.distsys.a2.utils.MapReduceUtils;
import com.distsys.a2.utils.MappedItem;

public class DistributedMapReduce {
    public static String[] DistributedMapReduceMethod(Map<String, String> input) {
        long distributedStart = System.currentTimeMillis();
        final Map<String, Map<String, Integer>> output = new HashMap<>();

        // MAP
        long distributedMapStart = System.currentTimeMillis();
        List<MappedItem> mappedItems = mapPhase(input);
        long distributedMapEnd = System.currentTimeMillis();
        String distributedMapTime = "Distributed Map Time: " + (distributedMapEnd - distributedMapStart) + "ms";

        // GROUP
        long distributedGroupStart = System.currentTimeMillis();
        Map<String, List<String>> groupedItems = groupPhase(mappedItems);
        long distributedGroupEnd = System.currentTimeMillis();
        String distributedGroupTime = "Distributed Group Time: " + (distributedGroupEnd - distributedGroupStart) + "ms";

        // REDUCE
        long distributedReduceStart = System.currentTimeMillis();
        reducePhase(groupedItems, output);
        long distributedReduceEnd = System.currentTimeMillis();
        String distributedReduceTime = "Distributed Reduce Time: " + (distributedReduceEnd - distributedReduceStart) + "ms";

        long distributedEnd = System.currentTimeMillis();
        String distributedTime = "Distributed Time: " + (distributedEnd - distributedStart) + "ms";

        String[] times = {distributedMapTime, distributedGroupTime, distributedReduceTime, distributedTime};
        return times;
    }

    static List<MappedItem> mapPhase(Map<String, String> input) {
        final List<MappedItem> mappedItems = new LinkedList<>();

        final MapCallback<String, MappedItem> mapCallback = new MapCallback<>() {
            @Override
            public synchronized void mapDone(String file, List<MappedItem> results) {
                mappedItems.addAll(results);
            }
        };

        List<Thread> mapCluster = new ArrayList<>(input.size());
        Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
        while(inputIter.hasNext()) {
            Map.Entry<String, String> entry = inputIter.next();
            final String file = entry.getKey();
            final String contents = entry.getValue();

            List<String> splitLines = splitLines(contents);
            int chunkSize = determineChunkSize(splitLines.size());
            createMapThreads(file, splitLines, chunkSize, mapCallback, mapCluster);
        }

        waitForThreads(mapCluster);
        return mappedItems;
    }

    static Map<String, List<String>> groupPhase(List<MappedItem> mappedItems) {
        Map<String, List<String>> groupedItems = new HashMap<>();
        Iterator<MappedItem> mappedIter = mappedItems.iterator();
        while(mappedIter.hasNext()) {
            MappedItem item = mappedIter.next();
            String word = item.getWord();
            String file = item.getFile();

            List<String> list = groupedItems.get(word);
            if (list == null) {
                list = new LinkedList<>();
                groupedItems.put(word, list);
            }
            list.add(file);
        }

        return groupedItems;
    }

    static void reducePhase(Map<String, List<String>> groupedItems, Map<String, Map<String, Integer>> output) {
        final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<>() {
            @Override
            public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
            }
        };

        // determine the batch size based on the total words
        int batchSize = determineBatchSize(groupedItems.size());
        List<Thread> reduceCluster = new ArrayList<>(groupedItems.size());

        // create batches of words to be processed by each reduce thread
        List<String> wordsBatch = new ArrayList<>();
        Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
        while(groupedIter.hasNext()) {
            Map.Entry<String, List<String>> entry = groupedIter.next();
            final String word = entry.getKey();
            final List<String> list = entry.getValue();

            wordsBatch.add(word);

            if(wordsBatch.size() >= batchSize || !groupedIter.hasNext()) {
                createReduceThreads(new ArrayList<>(wordsBatch), list, reduceCallback, reduceCluster);
                wordsBatch.clear();
            }
        }

        // wait for reducing phase to be over
        waitForThreads(reduceCluster);
    }

    private static List<String> splitLines(String contents) {
        String[] lines = contents.split("\n");

        // Split long lines (lines greater than 80 characters)
        List<String> splitLines = new ArrayList<>();
        for (String line : lines) {
            while(line.length() > 80) {
                int splitIndex = line.lastIndexOf(' ', 80);
                if (splitIndex == -1) {
                    splitIndex = 80;
                }

                String part = line.substring(0, splitIndex);
                splitLines.add(part);
                line = line.substring(splitIndex);
            }

            if (!line.isEmpty()) {
                splitLines.add(line);
            }
        }
        return splitLines;
    }

    private static void createMapThreads(String file, List<String> splitLines, int chunkSize, MapCallback<String, MappedItem> callback, List<Thread> mapCluster) {
        int start = 0;

        while (start < splitLines.size()) {
            int end = Math.min(start + chunkSize, splitLines.size());
            final String[] chunk = new String[end - start];
            System.arraycopy(splitLines.toArray(), start, chunk, 0, end - start);
            final String chunkContent = String.join("\n", chunk);

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    MapReduceUtils.map(file, chunkContent, callback);
                }
            });

            mapCluster.add(t);
            t.start();
            start = end;
        }
    }

    private static void createReduceThreads(List<String> wordsBatch, List<String> list, ReduceCallback<String, String, Integer> callback, List<Thread> reduceCluster) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (String word : wordsBatch) {
                    MapReduceUtils.reduce(word, list, callback);
                }
            }
        });
        reduceCluster.add(t);
        t.start();
    }

    private static void waitForThreads(List<Thread> threads) {
        for (Thread t : threads) {
            try {
                t.join();
            } catch(InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static int determineChunkSize(int totalLines) {
        int minChunkSize = 1_000;
        int maxChunkSize = 10_000;
        return Math.min(maxChunkSize, Math.max(minChunkSize, totalLines / 10));
    }

    private static int determineBatchSize(int totalWords) {
        int minBatchSize = 100;
        int maxBatchSize = 1_000;
        return Math.min(Math.max(minBatchSize, totalWords / 10), maxBatchSize);
    }
}

