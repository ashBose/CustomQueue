package com.qadium.cc;

import java.util.Map;
import java.util.HashMap;

/**
 * A container for information about sequences for the q service.
 *
 */
class SequenceData {

    private int queueNumber;
    private int lastSentPart;
    private Map<Integer, String> unsentParts;

    SequenceData() {
        queueNumber = -1;
        lastSentPart = -1;
        unsentParts = new HashMap<Integer, String>();
    }

    /**
     * Gets the queue number for this sequence.
     */
    int getQueueNumber() {
        return queueNumber;
    }

    /**
     * Adds a message to the unsent parts.
     */
    void addMessage(int partNum, String message, int queueNum) {
        if(queueNumber == -1) {
            queueNumber = queueNum;
        }
        unsentParts.put(partNum, message);
    }

    /**
     * Returns the next part to send, if there is one.
     */
    String getNextMessage() {
        String message = unsentParts.remove(lastSentPart + 1);
        if(message != null)
            lastSentPart += 1;
        return message;
    }

}
