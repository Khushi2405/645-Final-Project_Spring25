package com.database.finalproject.buffermanager;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageNotFoundException;
import com.database.finalproject.model.Row;
import com.database.finalproject.repository.Utilities;
import java.nio.file.Path;
import java.nio.file.Paths;

class BufferManagerE2ETest {
    private BufferManager bufferManager;

    @BeforeEach
    void setUp() {
        bufferManager = new BufferManagerImpl(5);
    }

    @Test
    void testPinStillInBuffer() {
        Path filePath = Paths.get("src", "main", "resources", "static", "title.basics.tsv");
        Utilities.loadDataset(bufferManager, "" + filePath.toAbsolutePath());
        Page page = bufferManager.getPage(1);
        int totalTime = 0;
        long totalIterations = 100;
        for (int i = 2; i <= 101; i++) {
            long startTime = System.nanoTime();
            page = bufferManager.getPage(i);
            long endTime = System.nanoTime();
            totalTime += endTime - startTime;
            bufferManager.unpinPage(i);
        }
        long avgTime = totalTime / totalIterations;
        long startTime = System.nanoTime();
        page = bufferManager.getPage(1);
        long endTime = System.nanoTime();
        long inBufferTime = endTime - startTime;
        assertTrue(inBufferTime < 0.5 * avgTime, "Page 1 should be in the buffer " + avgTime + " " + inBufferTime);

    }
}