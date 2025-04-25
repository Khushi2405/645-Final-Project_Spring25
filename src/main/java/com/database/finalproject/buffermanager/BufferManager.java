package com.database.finalproject.buffermanager;

import com.database.finalproject.model.page.Page;

public abstract class BufferManager {
    final int bufferSize;

    public BufferManager(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public abstract Page getPage(int pageId, int ...index);

    public abstract Page createPage(int ...index);

    public abstract void markDirty(int pageId, int ...index);

    public abstract void unpinPage(int pageId, int ...index);

    public abstract void writeToBinaryFile(Page page, int ...index);

    public abstract void force();

    public abstract String getRootPageId(int ...index);

    public abstract void setRootPageId(int rootPageId, int ...index);
    public abstract String getFilePath(int index);
}
