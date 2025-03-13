import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.Scanner;

public class MapReduceFiles {

  public static void main(String[] args) {
    long TotalStartTime = System.currentTimeMillis();
    List<String> timings = new ArrayList<>();

    if (args.length < 3) {
      System.err.println("usage: java MapReduceFiles file1.txt file2.txt file3.txt");
      return; // Return to Prevent Errors
    }

    Map<String, String> input = new HashMap<String, String>();
    try {
      long fileReadStart = System.currentTimeMillis();
      // Loops through the args (Files) and reads them into the input map
      for (String file : args) {
        input.put(file, readFile(file));
      }
      long fileReadEnd = System.currentTimeMillis();
      timings.add("File Read Time: " + (fileReadEnd - fileReadStart) + " ms");
    }
    catch (IOException ex)
    {
        System.err.println("Error reading files...\n" + ex.getMessage());
        ex.printStackTrace();
        System.exit(0);
    }

    // APPROACH #1: Brute force
    {
      long bruteForceStart = System.currentTimeMillis();
      Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        String file = entry.getKey();
        String contents = entry.getValue();

        String[] words = contents.trim().split("\\s+");

        for(String word : words) {

          Map<String, Integer> files = output.get(word);
          if (files == null) {
            files = new HashMap<String, Integer>();
            output.put(word, files);
          }

          Integer occurrences = files.remove(file);
          if (occurrences == null) {
            files.put(file, 1);
          } else {
            files.put(file, occurrences.intValue() + 1);
          }
        }
      }
      long bruteForceEnd = System.currentTimeMillis();
      timings.add("Brute Force Time: " + (bruteForceEnd - bruteForceStart) + " ms");
      // show me:
      System.out.println(output);
    }


    // APPROACH #2: MapReduce
    {
      long mapReduceStart = System.currentTimeMillis();
      Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

      // MAP:
      long mapStart = System.currentTimeMillis();
      List<MappedItem> mappedItems = new LinkedList<MappedItem>();

      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        String file = entry.getKey();
        String contents = entry.getValue();

        map(file, contents, mappedItems);
      }
      long mapEnd = System.currentTimeMillis();
      timings.add("Map Time: " + (mapEnd - mapStart) + " ms");

      // GROUP:
      long groupStart = System.currentTimeMillis();
      Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

      Iterator<MappedItem> mappedIter = mappedItems.iterator();
      while(mappedIter.hasNext()) {
        MappedItem item = mappedIter.next();
        String word = item.getWord();
        String file = item.getFile();
        List<String> list = groupedItems.get(word);
        if (list == null) {
          list = new LinkedList<String>();
          groupedItems.put(word, list);
        }
        list.add(file);
      }
      long groupEnd = System.currentTimeMillis();
      timings.add("Group Time: " + (groupEnd - groupStart) + " ms");

      // REDUCE:

      long reduceStart = System.currentTimeMillis();

      Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
      while(groupedIter.hasNext()) {
        Map.Entry<String, List<String>> entry = groupedIter.next();
        String word = entry.getKey();
        List<String> list = entry.getValue();

        reduce(word, list, output);
      }

      long reduceEnd = System.currentTimeMillis();
      timings.add("Reduce Time: " + (reduceEnd - reduceStart) + " ms");

      System.out.println(output);
    }


    // APPROACH #3: Distributed MapReduce
    {
      long distributedStart = System.currentTimeMillis();
      final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

      // MAP:

      long distributedMapStart = System.currentTimeMillis();

      final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

      final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
        @Override
        public synchronized void mapDone(String file, List<MappedItem> results) {
          mappedItems.addAll(results);
        }
      };

      List<Thread> mapCluster = new ArrayList<Thread>(input.size());

      Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
      while(inputIter.hasNext()) {
        Map.Entry<String, String> entry = inputIter.next();
        final String file = entry.getKey();
        final String contents = entry.getValue();


        String[] lines = contents.split("\n");

        // Split long lines (lines greater than 80 characters)
        List<String> splitLines = new ArrayList<>();
        for (String line : lines) {
          while (line.length() > 80) {
            // find the next whitespace
            int splitIndex = line.lastIndexOf(' ', 80);
            if (splitIndex == -1){
              splitIndex = 80; // force split if no whitespace
            }

            String part = line.substring(0, splitIndex);
            splitLines.add(part);
            line = line.substring(splitIndex).trim();
          }

          if (!line.isEmpty()) {
            splitLines.add(line);
          }
        }

        // dynamically adjust chunk size based on total lines
        int totalLines = splitLines.size();
        int chunkSize = Math.min(10_000, Math.max(1_000, totalLines / 10));

        int start = 0;

        while (start < splitLines.size()) {
          int end = Math.min(start + chunkSize, splitLines.size());
          final String[] chunk = new String[end - start];
          final String chunkContent = String.join("\n", chunk);
          System.arraycopy(splitLines.toArray(), start, chunk, 0, end - start);

          // create a thread for this chunk of lines
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              map(file, chunkContent, mapCallback);
            }
          });

          mapCluster.add(t);
          t.start();

          start = end;
        }
        
      }

      // wait for mapping phase to be over:
      for(Thread t : mapCluster) {
        try {
          t.join();
        } catch(InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      long distributedMapEnd = System.currentTimeMillis();
      timings.add("Distributed Map Time: " + (distributedMapEnd - distributedMapStart) + " ms");

      // GROUP:

      long distributedGroupStart = System.currentTimeMillis();
      Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

      Iterator<MappedItem> mappedIter = mappedItems.iterator();
      while(mappedIter.hasNext()) {
        MappedItem item = mappedIter.next();
        String word = item.getWord();
        String file = item.getFile();
        List<String> list = groupedItems.get(word);
        if (list == null) {
          list = new LinkedList<String>();
          groupedItems.put(word, list);
        }
        list.add(file);
      }

      long distributedGroupEnd = System.currentTimeMillis();
      timings.add("Distributed Group Time: " + (distributedGroupEnd - distributedGroupStart) + " ms");

      // REDUCE:

      long distributedReduceStart = System.currentTimeMillis();


      final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
        @Override
        public synchronized void reduceDone(String k, Map<String, Integer> v) {
          output.put(k, v);
        }
      };

      // determine the batch size based on total words
      int totalWords = groupedItems.size();
      int minBatchSize = 100;
      int maxBatchSize = 1000;
      int batchSize = Math.min(Math.max(minBatchSize, totalWords / 10), maxBatchSize);

      List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

      // Create batches of words to be processed by each reduce thread
      List<String> wordsBatch = new ArrayList<>();
      Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
      while(groupedIter.hasNext()) {
        Map.Entry<String, List<String>> entry = groupedIter.next();
        final String word = entry.getKey();
        final List<String> list = entry.getValue();

        // Add the word to the batch
        wordsBatch.add(word);

        if (wordsBatch.size() >= batchSize || !groupedIter.hasNext()) {
          // create a new thread for processing this batch
          List<String> batchWords = new ArrayList<>(wordsBatch);
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              Map<String, Map<String, Integer>> localOutput = new HashMap<>();
              for (String word : batchWords) {
                List<String> files = groupedItems.get(word);
                reduce(word, files, localOutput);
              }
              synchronized (output) {
                for (Map.Entry<String, Map<String, Integer>> entry : localOutput.entrySet()) {
                  output.put(entry.getKey(), entry.getValue());
                }
              }
            }
          });
        
        // Thread t = new Thread(new Runnable() {
        //   @Override
        //   public void run() {
        //     reduce(word, list, reduceCallback);
        //   }
        // });
          reduceCluster.add(t);
          t.start();

          wordsBatch.clear();
        }
      }

      // wait for reducing phase to be over:
      for(Thread t : reduceCluster) {
        try {
          t.join();
        } catch(InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      long distributedReduceEnd = System.currentTimeMillis();
      timings.add("Distributed Reduce Time: " + (distributedReduceEnd - distributedReduceStart) + " ms");

      long distributedEnd = System.currentTimeMillis();
      timings.add("Distributed Total Time: " + (distributedEnd - distributedStart) + " ms");
      System.out.println(output);

      long totalEndTime = System.currentTimeMillis();
      timings.add("Total Execution Time: " + (totalEndTime - TotalStartTime) + " ms");

      writeTimesToFile(timings);
    }
  }

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

  public static interface MapCallback<E, V> {

    public void mapDone(E key, List<V> values);
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

  public static interface ReduceCallback<E, K, V> {

    public void reduceDone(E e, Map<K,V> results);
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

  private static class MappedItem {

    private final String word;
    private final String file;

    public MappedItem(String word, String file) {
      this.word = word;
      this.file = file;
    }

    public String getWord() {
      return word;
    }

    public String getFile() {
      return file;
    }

    @Override
    public String toString() {
      return "[\"" + word + "\",\"" + file + "\"]";
    }
  }

  private static String readFile(String pathname) throws IOException {
    File file = new File(pathname);
    StringBuilder fileContents = new StringBuilder((int) file.length());
    Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
    String lineSeparator = System.getProperty("line.separator");

    try {
      if (scanner.hasNextLine()) {
        fileContents.append(scanner.nextLine());
      }
      while (scanner.hasNextLine()) {
        fileContents.append(lineSeparator + scanner.nextLine());
      }
      return fileContents.toString();
    } finally {
      scanner.close();
    }
  } 

  private static void writeTimesToFile(List<String> timings) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter("times_taken.txt", true))) {
        for (String time : timings) {
            writer.write(time);
            writer.newLine();
        }
        writer.write("------------------------------");
        writer.newLine();
    } catch (IOException e) {
        System.err.println("Error writing execution times: " + e.getMessage());
    }
}

}

