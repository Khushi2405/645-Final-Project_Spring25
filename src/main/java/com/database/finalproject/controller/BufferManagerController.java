package com.database.finalproject.controller;

import com.database.finalproject.model.Page;
import com.database.finalproject.service.BufferManagerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/buffer")
public class BufferManagerController {

    private final BufferManagerService bufferManagerService;

    @Autowired
    public BufferManagerController(BufferManagerService bufferManagerService) {
        this.bufferManagerService = bufferManagerService;
    }

    @GetMapping("/page/{pageId}")
    public Page getPage(@PathVariable int pageId) {
        return bufferManagerService.getPage(pageId);
    }

    @PostMapping("/page")
    public Page createPage() {
        return bufferManagerService.createPage();
    }

    @PostMapping("/page/{pageId}/mark-dirty")
    public void markDirty(@PathVariable int pageId) {
        bufferManagerService.markDirty(pageId);
    }

    @PostMapping("/page/{pageId}/unpin")
    public void unpinPage(@PathVariable int pageId) {
        bufferManagerService.unpinPage(pageId);
    }
}
