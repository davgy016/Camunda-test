package com.camundatest.utilities;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class BpmnFileScanner {
    
    public static List<String> findBpmnFiles(Path rootDir) throws IOException {
        if (rootDir == null) {
            throw new IllegalArgumentException("rootDir is null");
        }
        if (!Files.exists(rootDir)) {
            throw new NoSuchFileException("Not found: " + rootDir.toAbsolutePath());
        }

        try (Stream<Path> s = Files.walk(rootDir)) {
            return s.filter(Files::isRegularFile)
                    .filter(BpmnFileScanner::isBpmnFile)
                    .map(path -> path.getFileName().toString())
                    .sorted()
                    .collect(Collectors.toList());
        }
    }    

    private static boolean isBpmnFile(Path p) {
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
        return name.endsWith(".bpmn");
    }

 
    // public static void main(String[] args) throws IOException {
    //     List<String> bpmnFiles = BpmnFileScanner.findBpmnFiles(Paths.get("/workspaces/Camunda-test/testing/src/test/resources"));

    //     bpmnFiles.forEach(System.out::println);
    // }
}
