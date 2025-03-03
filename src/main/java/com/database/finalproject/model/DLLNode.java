package com.database.finalproject.model;

public class DLLNode {
    Page page;
    DLLNode next;
    DLLNode prev;
    boolean isDirty;
    int pinCount;

    DLLNode(Page page){
        this.page = page;
        next = null;
        prev = null;
        isDirty = false;
        pinCount = 0;
    }
}
