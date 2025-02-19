package com.database.finalproject.service;

import com.database.finalproject.model.Page;
import org.springframework.stereotype.Service;


public interface BufferManagerService {

    Page getPage(int pageId);

    Page createPage();

    void markDirty(int pageId);

    void unpinPage(int pageId);
}
