package com.distsys.a2.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestUtils {

    public static Map<String, String> readFilesIntoMap(String[] files) {
        Map<String, String> input = new HashMap<>();
        try {
            for (String file : files) {
                String content = new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemResource(file).toURI())));
                input.put(file, content);
            }
        } catch (IOException | URISyntaxException e) {
            System.err.println("Error reading files: " + e.getMessage());
            e.printStackTrace();
        }
        return input;
    }
}