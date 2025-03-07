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
    }

    @Override
    public Page getPage(int pageId) {
        // Logic to fetch a page from buffer
        System.out.println("hash Size " + pageHash.size());
        // if page id already in buffer then move it to front and increment the pin counter
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
                    //buffer manager is full throw error
                    System.out.println("Buffer manager is full cannot fetch new pages");
                    return null;
                };
            }
            try {
                Page page = readPage(pageId);
                if(page == null) return null;
                DLLNode currNode = new DLLNode(page);
                addNewPage(pageId, currNode);
                printDLL();
                return page;
            }
            catch (IOException e){
                System.out.println("Cannot read file");
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
        File file = new File(INPUT_FILE);

        if(maxPages == 0) {
            if (file.exists()) {
                long fileSize = file.length(); // Size in bytes
                System.out.println("File size: " + fileSize + " bytes");
                System.out.println("No of pages: " + fileSize / PAGE_SIZE);
                maxPages = (int) (fileSize / PAGE_SIZE);
            } else {
                System.out.println("File does not exist.");
            }
        }
        if(pageHash.size() > bufferSize-1){
            if(!removeLRUNode()){
                //buffer manager is full throw error
                System.out.println("Buffer manager is full cannot fetch new pages");
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

    @Override
    public void writeToBinaryFile(Page page) {
        try (RandomAccessFile raf = new RandomAccessFile(INPUT_FILE, "rw")){
            long offset = (long) (page.getPid()-1) * PAGE_SIZE;
            raf.seek(offset);
            for (int i = 0 ; i < PAGE_ROW_LIMIT; i++) {
                Row row = page.getRow(i);
                if(row != null){
                    byte[] movieId = truncateOrPadByteArray(row.movieId(), 9);
                    byte[] movieTitle = truncateOrPadByteArray(row.title(), 30);
                    raf.write(movieId);
                    raf.write(movieTitle);
                } else {
                    // If fewer entries are provided, write empty padded entries
                    byte[] emptyArray = new byte[39];
                    Arrays.fill(emptyArray, PADDING_BYTE);
                    raf.write(emptyArray);
                }
            }
            raf.write(EXTRA_BYTE);
            System.out.println("Updated page " + page.getPid() + " successfully!");

            //dos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void bringPageFront(DLLNode currNode) {
        //if node is already at front don't do anything
        if(currNode == headBufferPool) return;

        //if node is at tail just point the prev node to null
        else if(currNode == tailBufferPool){
            DLLNode prev = currNode.prev;
            prev.next = null;
            tailBufferPool = prev;
        }

        //if node in middle of list point to prev and next nodes to each other
        else{
            DLLNode prev = currNode.prev;
            DLLNode next = currNode.next;
            prev.next = next;
            next.prev = prev;
        }

        //now add the curr node to starting of list and change the head pointer
        currNode.prev = null;
        currNode.next = headBufferPool;
        headBufferPool.prev = currNode;
        headBufferPool = currNode;
    }

    private Page readPage(int pageId) throws IOException {
        Page page = new PageImpl(pageId);
        try(RandomAccessFile raf = new RandomAccessFile(INPUT_FILE, "r")){
            long offset = (long) (pageId-1) * PAGE_SIZE;
            System.out.println(raf.length()/PAGE_SIZE);
            System.out.println("length " + raf.length() + " offset " + offset);
            if(raf.length() <= offset){
                System.out.println("Page with page id : " + page.getPid() + " doesn't exist");
                return null;
            }
            raf.seek(offset);
            for(int i = 0 ; i < PAGE_ROW_LIMIT; i++){
                //System.out.println(i);
                if(raf.getFilePointer() < raf.length()){
                    byte[] movieId = new byte[9];
                    byte[] movieTitle = new byte[30];
                    raf.read(movieId);
                    raf.read(movieTitle);
                    movieId = removeTrailingBytes(movieId);
                    movieTitle = removeTrailingBytes(movieTitle);
                    if(movieId != null && movieTitle != null){
                        Row row = new Row(movieId, movieTitle);
                        page.insertRow(row);
//                        System.out.println(new String(movieId).trim());
//                        System.out.println(new String(movieTitle).trim());
                    }
                    else break;

                }
            }
            System.out.println(page.getRow(0));
            System.out.println(page.getRow(PAGE_ROW_LIMIT-1));

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
        boolean isArrayEmpty = true;
        for (int i = input.length - 1; i >= 0; i--) {
            if (input[i] != PADDING_BYTE) {  // Only remove our custom padding byte
                endIndex = i + 1;
                isArrayEmpty = false;
                break;
            }
        }
        return isArrayEmpty ? null : Arrays.copyOf(input, endIndex);
    }

    private static byte[] truncateOrPadByteArray(byte[] value, int maxLength) {

        if (value.length > maxLength) {
            return Arrays.copyOf(value, maxLength); // Truncate safely at byte level
        } else {
            byte[] padded = new byte[maxLength];
            System.arraycopy(value, 0, padded, 0, value.length); // Copy original bytes
            Arrays.fill(padded, value.length, maxLength, PADDING_BYTE); // Fill remaining space with 0x7F
            return padded;
        }


    }
}
