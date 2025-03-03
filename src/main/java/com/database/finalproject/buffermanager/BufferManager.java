package com.database.finalproject.buffermanager;

import com.database.finalproject.model.Page;

public abstract class BufferManager {
    final int bufferSize;

    public BufferManager(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public abstract Page getPage(int pageId);
    public abstract Page createPage();
    public abstract void markDirty(int pageId);
    public abstract void unpinPage(int pageId);
}
