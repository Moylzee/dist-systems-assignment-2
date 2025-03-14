package a2.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import a2.callback.MapCallback;
import a2.callback.ReduceCallback;

public class MapReduceUtils {
    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for(String word: words) {
            // Remove punctuation and non-text symbols
            String cleanedWord = word.replaceAll("[^a-zA-Z]", "");
            if (!cleanedWord.isEmpty()) {
                mappedItems.add(new MappedItem(cleanedWord, file));
            }
        }
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
        // Remove punctuation and non-text symbols
        String cleanedWord = word.replaceAll("[^a-zA-Z]", "");
        if (!cleanedWord.isEmpty()) {
            results.add(new MappedItem(cleanedWord, file));
        }
        }
        callback.mapDone(file, results);
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
        Integer occurrences = reducedList.get(file);
        if (occurrences == null) {
            reducedList.put(file, 1);
        } else {
            reducedList.put(file, occurrences.intValue() + 1);
        }
        }
        callback.reduceDone(word, reducedList);
    }
}
