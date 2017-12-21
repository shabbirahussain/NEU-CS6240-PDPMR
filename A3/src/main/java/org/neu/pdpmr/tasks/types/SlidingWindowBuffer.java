package org.neu.pdpmr.tasks.types;

import java.util.*;

/**
 * This class is a blackbox buffer class that stores k scores in a sliding buffer.
 * It stores input words into a buffer. Next method is used to move buffer forward.
 * It will automatically assign magic properties exitingWord, currWord, newCurrWord,
 * enteringWord.
 * @author shabbir.ahussain
 */
public class SlidingWindowBuffer {
    private int k;
    private Deque<String> backwardQ; // Queue for looking back.
    private Deque<String> forwardQ;  // Current word is at the end of this queue.
    private Queue<String> inpBuffQ;  // Queue for unseen inputs.

    public String exitingWord, currWord, newCurrWord, enteringWord;

    /**
     * Default constructor
     * @param k is the neighborhood size.
     * @Exception When k is less than 1.
     */
    public SlidingWindowBuffer(int k)
        throws Exception{
        if (k<1) throw new Exception("K should be greater than 1.");

        this.k = k;
        backwardQ = new LinkedList<>();
        forwardQ  = new LinkedList<>();
        inpBuffQ  = new LinkedList<>();
        exitingWord = currWord = newCurrWord = enteringWord = "";
    }

    public void enqueue(Collection<String> elem){
        inpBuffQ.addAll(elem);
    }

    public boolean hasMoreWords(){
        return !inpBuffQ.isEmpty();
    }

    /**
     * Moves the windows forward and sets the public properties:
     * exitingWord, currWord, newCurrWord, enteringWord.
     */
    public void next(){
        exitingWord = currWord = newCurrWord = enteringWord = "";
        boolean isBuffEmpty = inpBuffQ.isEmpty();

        if (backwardQ.size() == k) {
            exitingWord = backwardQ.poll();
        }
        if (!isBuffEmpty) {
            enteringWord = inpBuffQ.poll();
            forwardQ.add(enteringWord);
        }
        if (forwardQ.size() == k + 2||
                (isBuffEmpty && !forwardQ.isEmpty())){
            currWord = forwardQ.poll();
            backwardQ.add(currWord);
            newCurrWord = forwardQ.peek();
        } else if (forwardQ.size() == k + 1){
            newCurrWord = forwardQ.peek();
        }
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("{ -=" +  exitingWord
                        + ", +=" + currWord
                        + ", -=" + newCurrWord
                        + ", +=" + enteringWord
                        + " } : ");
        sb.append(backwardQ);sb.append(" <- ");
        sb.append(forwardQ);sb.append(" <- ");
        sb.append(inpBuffQ );

        return sb.toString();
    }
}
