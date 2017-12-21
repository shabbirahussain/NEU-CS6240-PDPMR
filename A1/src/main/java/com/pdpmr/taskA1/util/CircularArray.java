package com.pdpmr.taskA1.util;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * @author shabbir.ahussain
 * @param <T>
 */
class CircularArray<T>{
    private int maxSize, size;
    private T[] buffer;
    private int p, q;

    @SuppressWarnings("unchecked")
    public CircularArray(Class<T> c, final int maxSize){
        this.maxSize = Math.max(maxSize, 0);
        buffer = (T[]) Array.newInstance(c, this.maxSize);
        p = 0;
        q = -1;
        size = 0;
    }

    /**
     * Enqueues a word at the end of the queue till the queue is full.
     * @param word is the word to enqueue.
     */
    public void addLast(final T word) throws ArrayIndexOutOfBoundsException{
        if (size == maxSize) {
            throw new ArrayIndexOutOfBoundsException();
        }
        q = ((++q) == maxSize)? 0 : q;
        size ++;
        this.buffer[q] = word;
    }

    public T removeFirst(){
        if(size == 0){ // Queue is empty
            throw new ArrayIndexOutOfBoundsException();
        }
        T temp = this.buffer[p];
        p = ((++p) == maxSize)? 0 : p;
        size --;
        return temp;
    }

    public T get(int index) throws ArrayIndexOutOfBoundsException{
        index = (index + p);
        if (q < p) {
            q += maxSize; // Normalize q to match expectation.
            if (this.size() == 0 || !(p <= index && index <= q)) {
                throw new ArrayIndexOutOfBoundsException();
            }
            q -= maxSize; // Put q back to normal value.
            index %= maxSize;
        }

        return this.buffer[index];
    }

    public int getRemainingCapacity(){
        return this.maxSize - this.size();
    }

    public int size(){
        return this.size;
    }

    @Override
    public String toString() {
        return Arrays.asList(buffer).toString() + " (p=" + p + ", q=" + q + ")";
    }
}