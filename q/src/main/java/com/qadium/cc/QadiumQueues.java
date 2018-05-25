package com.qadium.cc;

import com.google.gson.reflect.TypeToken;
import com.google.gson.Gson;
import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.Base64;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * An implementation of q, a simple message delivery service.
 */
public class QadiumQueues implements QadiumCodingChallenge {

    private List<Queue<String>> queues;
    private static final int NUM_QUEUES = 5;
    private static final Type MESSAGE_TYPE = new TypeToken<Map<String, Object>>() {}.getType();
    private Function<Object, Object> transformRules;
    private Function<Map<String, Object>, Integer> dispatchRules;
    private Map<String, SequenceData> sequences;

    public QadiumQueues() {
        // Initialize the queues.
        queues = new ArrayList<Queue<String>>();
        for(int idx = 0; idx < NUM_QUEUES; idx++)
            queues.add(new LinkedList<String>());

        this.registerTransformRules();
        this.registerDispatchRules();

        sequences = new HashMap<String, SequenceData>();
    }

    /**
     * Transforms a message and enqueues it on one of five queues.
     */
    public void enqueue(String msg) throws QadiumException {
        Map<String, Object> msgMap = QadiumQueues.deserializeMessage(msg);
        this.transform(msgMap);
        int queueNum = dispatchRules.apply(msgMap);
        String result = QadiumQueues.serializeMessage(msgMap);

        Object sequenceId = msgMap.get("_sequence");
        Object partNum = msgMap.get("_part");
        if(sequenceId != null && sequenceId instanceof String &&
           partNum != null && QadiumQueues.isInteger(partNum)) {
            this.dispatchSequence(queueNum, result, (String)sequenceId, ((Double)partNum).intValue());
        } else {
            this.dispatch(queueNum, result);
        }
    }

    /**
     * Retrieves and removes the next element on a queue.
     *
     * Throws a QadiumException if the requested queue is empty.
     */
    public String next(int queueNumber) throws QadiumException {
        //queueNumber not checked
        String msg = queues.get(queueNumber).poll();
        if(msg == null)
            throw new QadiumException("The requested queue is empty.");
        return msg;
    }

    /**
     * Transform a message according to the registered transform rules.
     */
    private void transform(Map<String, Object> msgMap) throws QadiumException {
        // Apply transformation rules on all non-private fields.
        msgMap.replaceAll((k, v) -> transformRules.apply(v));

        // Apply hash transformation rule.
        String fieldToHash = (String)msgMap.get("_hash");
        if(fieldToHash != null) {
            try {
                putHash(msgMap, fieldToHash);
            } catch(NoSuchAlgorithmException e) {
                throw new QadiumException("Could not hash field because SHA-256 hashing is unavailable in this JVM.");
            }
        }
    }

    /**
     * Dispatch the message to the proper queue according to the dispatch rules.
     */
    private void dispatch(int queueNum, String msg) {
        queues.get(queueNum).add(msg);
    }

    /**
     * Dispatch all sendable messages in the sequence.
     */
    private void dispatchSequence(int queueNum, String msg, String sequenceId, int partNum) {
        if(!sequences.containsKey(sequenceId))
            sequences.put(sequenceId, new SequenceData());
        SequenceData sequence = sequences.get(sequenceId);
        sequence.addMessage(partNum, msg, queueNum);
        int sequenceQueueNum = sequence.getQueueNumber();
        for(msg = sequence.getNextMessage(); msg != null; msg = sequence.getNextMessage()) {
            // Note: if we got here, queueNum is properly set because part 0 has been sent.
            this.dispatch(sequenceQueueNum, msg);
        }
    }

    /**
     * Puts the base64-encoded SHA-256 digest of the UTF-8 encoded value of the specified
     * field in msgMap under the key "hash".
     */
    private void putHash(Map<String, Object> msgMap, String fieldToHash) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] unhashedField = ((String)(msgMap.get(fieldToHash))).getBytes(StandardCharsets.UTF_8);
        byte[] hashedField = digest.digest(unhashedField);
        msgMap.put("_hash", Base64.getEncoder().encodeToString(hashedField));
    }

    /**
     * Deserialize a JSON string into a Map.
     */
    private static Map<String, Object> deserializeMessage(String msg) {
        return new Gson().fromJson(msg, MESSAGE_TYPE);
    }

    /**
     * Serialize a Map into a JSON string.
     */
    private static String serializeMessage(Map<String, Object> msgMap) {
        return new Gson().toJson(msgMap, MESSAGE_TYPE);
    }

    /**
     * Returns whether or not a field name is private.
     */
    private static boolean isPrivateField(String fieldName) {
        return fieldName.startsWith("_");
    }

    /**
     * Registers transform rules.
     */
    private void registerTransformRules() {
        transformRules = Function.identity();
        // Reverse string if it contains "Qadium"
        this.registerTransformRule(o -> {
            if(o instanceof String && ((String)o).contains("Qadium"))
                return new StringBuffer(((String)o)).reverse().toString();
            else
                return o;
        });
        // Bitwise negate int values
        this.registerTransformRule(o -> QadiumQueues.isInteger(o) ? ~((Double)o).intValue() : o);
    }

    /**
     * Registers a new transform rule.
     */
    private void registerTransformRule(Function<Object, Object> rule) {
        transformRules = rule.compose(transformRules);
    }

    /**
     * Registers dispatch rules.
     */
    private void registerDispatchRules() {
        this.registerDispatchRule(4, msgMap -> true);
        this.registerDispatchRule(3, buildFieldWiseDispatchRule(o -> QadiumQueues.isInteger(o)));
        this.registerDispatchRule(2, buildFieldWiseDispatchRule(
                    o -> o instanceof String && ((String)o).contains("muidaQ")));
        this.registerDispatchRule(1, msgMap -> msgMap.containsKey("hash"));
        this.registerDispatchRule(0, msgMap -> msgMap.containsKey("_special"));
    }

    /**
     * Registers a new dispatch rule.
     */
    private void registerDispatchRule(int queueNum, Predicate<Map<String, Object>> condition) {
        Function<Map<String, Object>, Integer> prevDispatchRules = dispatchRules;
        dispatchRules = msgMap -> condition.test(msgMap) ? queueNum : prevDispatchRules.apply(msgMap);
    }

    /**
     * Builds a predicate over a message map.
     */
    private Predicate<Map<String, Object>> buildFieldWiseDispatchRule(Predicate<Object> condition) {
        return msgMap -> msgMap.entrySet()
                               .stream()
                               .filter(e -> !QadiumQueues.isPrivateField(e.getKey()))
                               .anyMatch(e -> condition.test(e.getValue()));
    }

    /**
     * Returns whether an object is an integer.
     *
     * An object is an integer if it has type Integer or has type Double
     * and an integral value.
     */
    private static boolean isInteger(Object o) {
        return o instanceof Integer ||
            (o instanceof Double
             && (Double)o == Math.floor((Double)o)
             && !Double.isInfinite((Double)o));
    }

}
