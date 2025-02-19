package com.database.finalproject.service;

import com.database.finalproject.buffer.BufferManager;
import com.database.finalproject.model.Page;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BufferManagerServiceImpl implements BufferManagerService{

    private final BufferManager bufferManager;

    @Autowired
    public BufferManagerServiceImpl(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    @Override
    public Page getPage(int pageId) {
        return bufferManager.getPage(pageId);
    }

    @Override
    public Page createPage() {
        return bufferManager.createPage();
    }

    @Override
    public void markDirty(int pageId) {
        bufferManager.markDirty(pageId);
    }

    @Override
    public void unpinPage(int pageId) {
        bufferManager.unpinPage(pageId);
    }
}
