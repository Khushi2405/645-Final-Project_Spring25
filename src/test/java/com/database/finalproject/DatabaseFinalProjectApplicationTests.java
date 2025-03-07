package com.database.finalproject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.database.finalproject.buffermanager.BufferManager;
import com.database.finalproject.buffermanager.BufferManagerImpl;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.Row;

@SpringBootTest
class DatabaseFinalProjectApplicationTests {

	@Test
	void contextLoads() {
	}

	private BufferManager bufferManager;

	@BeforeEach
	void setUp() {
		bufferManager = new BufferManagerImpl(5);
	}

	@Test
	void testCreatePage() {
		Page page = bufferManager.createPage();
		assertNotNull(page, "Page should be created successfully");
	}

	@Test
	void testInsertRow() {
		Page page = bufferManager.createPage();
		Row row = new Row("tt1111111".getBytes(StandardCharsets.UTF_8),
				"Test Movie Insert".getBytes(StandardCharsets.UTF_8));

		int rowId = page.insertRow(row);
		assertEquals(0, rowId - 1, "Row should be inserted at index 0");
	}

	@Test
	void testFetchPage() {
		Page createdPage = bufferManager.createPage();
		int pageId = createdPage.getPid();

		Page fetchedPage = bufferManager.getPage(pageId);
		assertNotNull(fetchedPage, "Fetched page should not be null");
		assertEquals(pageId, fetchedPage.getPid(), "Fetched page ID should match");
	}

	// @Test
	// void testMarkDirtyOnCreatePage() {
	// Page page = bufferManager.createPage();
	// int pageId = page.getPid();

	// bufferManager.markDirty(pageId);
	// // Verify dirty page logic, could be a flag inside BufferManager
	// }

}
