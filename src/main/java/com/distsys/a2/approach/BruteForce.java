package a2.approach;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class BruteForce {
    public static String BruteForceMethod(Map<String, String> input) {
        long bruteForceStart = System.currentTimeMillis();
        Map<String, Map<String, Integer>> output = new HashMap<>();

        Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();

        while(inputIter.hasNext()) {
            Map.Entry<String, String> entry = inputIter.next();
            String file = entry.getKey();
            String contents = entry.getValue();

            String[] words = contents.trim().split("\\s+");

            for (String word : words) {
                Map<String, Integer> files = output.get(word);

                if(files == null) {
                    files = new HashMap<>();
                    output.put(word, files);
                }

                Integer occurrences = files.remove(file);
                if (occurrences == null) {
                    files.put(file, 1);
                } else {
                    files.put(file, occurrences + 1);
                }
            }
        }

        long bruteForceEnd = System.currentTimeMillis();
        System.out.println(output);

        return "Brute Force Time: " + (bruteForceEnd - bruteForceStart) + "ms";
    }
}
