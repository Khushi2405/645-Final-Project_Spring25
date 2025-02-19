package com.database.finalproject.buffer;
import com.database.finalproject.model.Page;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.HashMap;

@Component
public class BufferManagerImpl extends BufferManager {

    private final Map<Integer, Page> pageBuffer;

    public BufferManagerImpl(@Value("${buffer.size:1024}")int bufferSize) {
        super(bufferSize);
        this.pageBuffer = new HashMap<>(bufferSize);
    }

    @Override
    public Page getPage(int pageId) {
        // Logic to fetch a page from buffer
        return null;
    }

    @Override
    public Page createPage() {
        // Logic to create a new page
        return null;
    }

    @Override
    public void markDirty(int pageId) {
        // Mark page as dirty
    }

    @Override
    public void unpinPage(int pageId) {
        // Unpin page
    }
}
