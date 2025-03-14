package a2.approach;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import a2.utils.MappedItem;
import a2.utils.MapReduceUtils;

public class MapReduce {
    public static String[] MapReduceMethod(Map<String, String> input) {

        Map<String, Map<String, Integer>> output = new HashMap<>();
        
        // Map
        long mapStart = System.currentTimeMillis();
        List<MappedItem> mappedItems = mapPhase(input);
        long mapEnd = System.currentTimeMillis();
        String mapTime = "Map Time: " + (mapEnd - mapStart) + "ms";

        // GROUP:
        long groupStart = System.currentTimeMillis();
        Map<String, List<String>> groupedItems = groupPhase(mappedItems);
        long groupEnd = System.currentTimeMillis();
        String groupTime = "Group Time: " + (groupEnd - groupStart) + "ms";

        // REDUCE:
        long reduceStart = System.currentTimeMillis();
        reducePhase(groupedItems, output);
        long reduceEnd = System.currentTimeMillis();
        String reduceTime = "Reduce Time: " + (reduceEnd - reduceStart) + "ms";

        System.out.println(output);

        String[] times = {mapTime, groupTime, reduceTime};
        return times;
    }

    private static List<MappedItem> mapPhase(Map<String, String> input) {
        List<MappedItem> mappedItems = new LinkedList<>();

        Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
        while(inputIter.hasNext()) {
            Map.Entry<String, String> entry = inputIter.next();
            String file = entry.getKey();
            String contents = entry.getValue();

            MapReduceUtils.map(file, contents, mappedItems);
        }
        return mappedItems;
    }

    private static Map<String, List<String>> groupPhase(List<MappedItem> mappedItems) {
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


    private static void reducePhase(Map<String, List<String>> groupedItems, Map<String, Map<String, Integer>> output) {
        Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
        while(groupedIter.hasNext()) {
            Map.Entry<String, List<String>> entry = groupedIter.next();
            String word = entry.getKey();
            List<String> list = entry.getValue();

            MapReduceUtils.reduce(word, list, output);
        }
    }
}
