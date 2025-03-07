package com.database.finalproject.buffermanager;
import com.database.finalproject.model.DLLNode;
import com.database.finalproject.model.Page;
import com.database.finalproject.model.PageImpl;
import com.database.finalproject.model.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import static com.database.finalproject.constants.PageConstants.*;

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
        File file = new File("src/main/resources/static/binary_heap.bin");

        if (file.exists()) {
            long fileSize = file.length(); // Size in bytes
            System.out.println("File size: " + fileSize + " bytes");
            System.out.println("No of pages: " + fileSize/PAGE_SIZE);
            maxPages = (int)fileSize/PAGE_SIZE;
        } else {
            System.out.println("File does not exist.");
        }
    }

    @Override
    public Page getPage(int pageId) {
        // Logic to fetch a page from buffer
        System.out.println("hash Size " + pageHash.size());
        if(pageHash.containsKey(pageId)){
            DLLNode currNode = pageHash.get(pageId);
            bringPageFront(currNode);
            currNode.pinCount++;
            printDLL();
            return currNode.page;
        }
        else{
            if(pageHash.size() > bufferSize-1){
                //implement LRU
                if(!removeLRUNode()){
                    //throw error
                    System.out.println("error in LRU");
                    return null;
                };
            }
            try {
                Page page = readPage(pageId);
                DLLNode currNode = new DLLNode(page);
                addNewPage(pageId, currNode);
                printDLL();
                return page;
            }
            catch (IOException e){
                System.out.println("Page with page id " + pageId + " not found");
                return null;
            }

        }
    }

    private void printDLL() {
        DLLNode temp = headBufferPool;
        while(temp != null){
            System.out.print(temp.page.getPid() + " ");
            temp = temp.next;
        }
        System.out.println();
    }

    @Override
    public Page createPage() {
        // Logic to create a new page
        if(pageHash.size() > bufferSize-1){
            if(!removeLRUNode()){
                //throw error
                return null;
            };
        }
        Page page = new PageImpl(++maxPages);
        DLLNode currNode = new DLLNode(page);
        addNewPage(page.getPid(), currNode);
        printDLL();
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
    @Override
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


    public Page readPage(int pageId) throws IOException {
        String inputFile = "src/main/resources/static/binary_heap.bin";
        Page page = new PageImpl(pageId);
        try(RandomAccessFile raf = new RandomAccessFile(inputFile, "r")){
            long offset = (long) (pageId-1) * PAGE_SIZE;
            System.out.println(raf.length()/PAGE_SIZE);
            System.out.println("length " + raf.length() + " offset " + offset);
            if(raf.length() <= offset){
                System.out.println("End of file reached");
                return null;
            }
                raf.seek(offset);
            for(int i = 0 ; i < PAGE_ROW_LIMIT; i++){
                //System.out.println(i);
                if(raf.getFilePointer() < raf.length()){
                    byte[] moveId = new byte[9];
                    byte[] movieTitle = new byte[30];
                    raf.read(moveId);
                    raf.read(movieTitle);
                    moveId = removeTrailingBytes(moveId);
                    movieTitle = removeTrailingBytes(movieTitle);
                    if(moveId[0] != PADDING_BYTE && movieTitle[0] != PADDING_BYTE){
                        Row row = new Row(moveId, movieTitle);
                        page.insertRow(row);
                        System.out.println(new String(moveId).trim());
                        System.out.println(new String(movieTitle).trim());
                    }

                }
            }
        }
        return page;
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
                writeToBinaryFile(unpinnedNode.page);
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

    public void writeToBinaryFile(Page page) {
        page.updateBinaryFile();
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

    private static byte[] removeTrailingBytes(byte[] input) {
        int endIndex = input.length;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] != PADDING_BYTE) {  // Only remove our custom padding byte
                endIndex = i + 1;
                break;
            }
        }
        return Arrays.copyOf(input, endIndex);
    }
}
