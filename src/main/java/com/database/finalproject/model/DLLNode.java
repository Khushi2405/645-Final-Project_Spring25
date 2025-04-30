package com.database.finalproject.model;

import com.database.finalproject.model.page.Page;

public class DLLNode {
    public Page page;
    public DLLNode next;
    public DLLNode prev;
    public boolean isDirty;
    public int pinCount;
    public int index;

    public DLLNode(Page page, int index){
        this.index = index;
        this.page = page;
        next = null;
        prev = null;
        isDirty = false;
        pinCount = 0;
    }
}
