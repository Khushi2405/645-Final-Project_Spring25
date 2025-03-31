package com.database.finalproject.model;

public class Rid {
    int pageId;
    int slotId;

    public int getPageId() {
        return pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public int getSlotId() {
        return slotId;
    }

    public void setSlotId(int slotId) {
        this.slotId = slotId;
    }

    public Rid(int pageId, int slotId) {
        this.pageId = pageId;
        this.slotId = slotId;
    }
}
