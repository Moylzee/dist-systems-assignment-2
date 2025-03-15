package com.distsys.a2.approach;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.distsys.a2.utils.MappedItem;
import com.distsys.a2.utils.TestUtils;

public class DistributedMapReduceTest {

    String[] files = {"txt2.txt", "txt3.txt", "txt4.txt"};

    @Test
    public void testMapPhase() {
        Map<String, String> input = TestUtils.readFilesIntoMap(files);

        List<MappedItem> mappedItems = DistributedMapReduce.mapPhase(input);

        assertNotNull(mappedItems);
        assertTrue(mappedItems.size() > 0, "Mapped items should not be empty");
    }

    @Test
    public void testGroupPhase() {
        Map<String, String> input = TestUtils.readFilesIntoMap(files);

        List<MappedItem> mappedItems = DistributedMapReduce.mapPhase(input);
        Map<String, List<String>> groupedItems = DistributedMapReduce.groupPhase(mappedItems);

        assertNotNull(groupedItems);
        assertTrue(groupedItems.size() > 0, "Grouped items should not be empty");
    }

    @Test
    public void testReducePhase() {
        Map<String, String> input = TestUtils.readFilesIntoMap(files);

        List<MappedItem> mappedItems = DistributedMapReduce.mapPhase(input);
        Map<String, List<String>> groupedItems = DistributedMapReduce.groupPhase(mappedItems);
        Map<String, Map<String, Integer>> output = new HashMap<>();

        DistributedMapReduce.reducePhase(groupedItems, output);

        assertNotNull(output);
        assertTrue(output.size() > 0, "Output should not be empty");
    }

    @Test
    public void testDistributedMapReduceMethod() {
        Map<String, String> input = TestUtils.readFilesIntoMap(files);

        String[] times = DistributedMapReduce.DistributedMapReduceMethod(input);

        assertNotNull(times);
        assertEquals(4, times.length);
        for (String time : times) {
            assertNotNull(time);
            assertTrue(time.contains("ms"));
        }
    }

    @Test
    public void testEmptyInput() {
        Map<String, String> input = new HashMap<>();

        String[] times = DistributedMapReduce.DistributedMapReduceMethod(input);

        assertNotNull(times);
        assertEquals(4, times.length);
        for (String time : times) {
            assertNotNull(time);
            assertTrue(time.contains("ms"));
        }
    }
}