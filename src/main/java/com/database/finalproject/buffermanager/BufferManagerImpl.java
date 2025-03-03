package com.database.finalproject.buffermanager;
import com.database.finalproject.model.DLLNode;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.HashMap;

@Component
public class BufferManagerImpl extends BufferManager {

    DLLNode headBufferPool, tailBufferPool;
    //DLLNode unpinnedPages;
    Map<Integer, DLLNode> pageHash;
    int maxPages;

    public BufferManagerImpl(@Value("${buffer.size:1024}")int bufferSize) {
        super(bufferSize);
        this.pageHash = new HashMap<>();
        tailBufferPool = null;
        headBufferPool = null;
        maxPages = 0;
    }

    @Override
    public Object getPage(int pageId) {
        // Logic to fetch a page from buffer
        if(pageHash.containsKey(pageId)){
            DLLNode currNode = pageHash.get(pageId);
            bringPageFront(currNode);
            currNode.pinCount++;
            return currNode.page;
        }
        else{
            if(pageHash.size() > bufferSize){
                //implement LRU
                if(!removeLRUNode()){
                    //throw error
                    return null;
                };
            }
            Page page = readPage(pageId);
            DLLNode currNode = new DLLNode(page);
            addNewPage(pageId, currNode);
            return page;

        }
    }

    @Override
    public Page createPage() {
        // Logic to create a new page
        if(pageHash.size() > bufferSize){
            if(!removeLRUNode()){
                //throw error
                return null;
            };
        }
        Page page = new PageImpl(++maxPages);
        DLLNode currNode = new DLLNode(page);
        addNewPage(page.getPid(), currNode);
        return page;
    }

    @Override
    public void markDirty(int pageId) {
        // Mark page as dirty
        if(pageHash.containsKey(pageId)){
            pageHash.get(pageId).isDirty = true;
            return;
        }
        System.out.println("No such page id");

    }

    @Override
    public void unpinPage(int pageId) {
        // Unpin page
        if(pageHash.containsKey(pageId)){
            pageHash.get(pageId).pinCount--;
            return;
        }
        System.out.println("No such page id");

    }
    public Page createPageToLoadDataset(){
        return new PageImpl(++maxPages);
    }

    private void bringPageFront(DLLNode curr) {
        if(curr == headBufferPool) return;
        else if(curr == tailBufferPool){
            DLLNode prev = curr.prev;
            prev.next = null;
            tailBufferPool = prev;
        }
        else{
            DLLNode prev = curr.prev;
            DLLNode next = curr.next;
            prev.next = next;
            next.prev = prev;
        }
        curr.prev = null;
        curr.next = headBufferPool;
        headBufferPool.prev = curr;
        headBufferPool = curr;
    }


    private Page readPage(int pageId) {
        //read page from memory
        return null;
    }

    private boolean removeLRUNode() {
        DLLNode unpinnedNode = tailBufferPool;
        while(unpinnedNode != null && unpinnedNode.pinCount != 0){
            unpinnedNode = unpinnedNode.prev;
        }
        if(unpinnedNode == null){
            //no such page with pincount 0
            //throw erroe
            return false;
        }
        else{
            if(unpinnedNode.isDirty){
                //write back to memory
            }
            pageHash.remove(unpinnedNode.page.getPid());
            DLLNode prev = unpinnedNode.prev;
            DLLNode next = unpinnedNode.next;
            if(unpinnedNode == tailBufferPool){
                prev.next = null;
                tailBufferPool = prev;
            }
            else if(unpinnedNode == headBufferPool){
                headBufferPool = next;
            }
            else{
                prev.next = next;
                next.prev = prev;
            }
        }
        return true;
    }

    private void addNewPage(int pageId, DLLNode currNode) {
        pageHash.put(pageId, currNode);
        if(headBufferPool == tailBufferPool && tailBufferPool == null){
            headBufferPool = currNode; tailBufferPool = currNode;
        }
        else{
            currNode.prev = null;
            currNode.next = headBufferPool;
            headBufferPool.prev = currNode;
            headBufferPool = currNode;

        }
        currNode.pinCount++;
    }
}
