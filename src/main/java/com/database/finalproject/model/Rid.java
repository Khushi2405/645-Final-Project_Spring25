package com.database.finalproject.model;

import java.io.*;

// Record Identifier
public class Rid implements Serializable {
    int pid; // Page ID
    int sid; // Slot ID within the page

    public Rid(int pid, int sid) {
        this.pid = pid;
        this.sid = sid;
    }
}