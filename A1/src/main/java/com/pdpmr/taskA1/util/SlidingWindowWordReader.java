package com.pdpmr.taskA1.util;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class SlidingWindowWordReader {
    private Scanner scanner;
    private LinkedList<String> tokenBuffer;
    private final int BUFFER_SIZE = 100;

    public SlidingWindowWordReader(Path path, String delimiter) throws IOException {
        this.tokenBuffer = new LinkedList<>();
        scanner = new Scanner(path).useDelimiter(delimiter);
        loadNextBytesIntoBuffer();
    }

    /**
     * @return true iff there are more elements in buffer.
     */
    public boolean hasMoreElements(){
        return this.tokenBuffer.size() > 0;
    }

    /**
     * Removes the first element from the buffer.
     */
    public void removeFirst(){
        this.tokenBuffer.removeFirst();
    }

    /**
     * Gets the item at given position from the buffer. If buffer is empty tries to fetch next bytes from file.
     * If nothing is left to read then returns empty string.
     * @param position is the position of word to read from.
     * @return Word from the buffer.
     */
    public String get(int position){
        if (position < 0) return "";
        if (this.tokenBuffer.size() - 1  <= position){
            // Try loading more bytes into buffer
            loadNextBytesIntoBuffer();
            if (this.tokenBuffer.size() <= position)
                return "";
        }
        return this.tokenBuffer.get(position);
    }

    /**
     * Reads more bytes from the file into its memory buffer.
     */
    private void loadNextBytesIntoBuffer(){
        while(this.tokenBuffer.size()<BUFFER_SIZE) {
            if (!scanner.hasNext()) return;
            String word = scanner
                    .next()
                    .toLowerCase()
                    .trim();

            if (!word.equals(""))
                this.tokenBuffer.addLast(word);
        }
    }

}