package com.database.finalproject.model;

public class DLLNode {
    public Page page;
    public DLLNode next;
    public DLLNode prev;
    public boolean isDirty;
    public int pinCount;

    public DLLNode(Page page){
        this.page = page;
        next = null;
        prev = null;
        isDirty = false;
        pinCount = 0;
    }
}
