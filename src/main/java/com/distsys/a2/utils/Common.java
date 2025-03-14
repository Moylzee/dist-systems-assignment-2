package a2.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class Common {
 
    public static String readFile(String filename) throws IOException {
        File file = new File(filename);
        StringBuilder contents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
        String lineSeparator = System.getProperty("line.separator");

        // Best effort - ignore exceptions
        try {
            if (scanner.hasNextLine()) {
                contents.append(scanner.nextLine());
            }
            while (scanner.hasNextLine()) {
                contents.append(lineSeparator + scanner.nextLine());
            }
            return contents.toString();
        } finally {
            scanner.close();
        }
    }


    public static void writeTimesToFile(List<String> timings) {
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
