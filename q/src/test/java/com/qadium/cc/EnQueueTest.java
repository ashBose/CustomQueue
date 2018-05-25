package com.qadium.cc;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.*;


public class EnQueueTest {

    static QadiumQueues queues = null;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        queues = new QadiumQueues();
    }

    @Test(expected = com.google.gson.JsonSyntaxException.class)
    public void testInputNonJson() throws QadiumException{
        queues.enqueue("abc");
    }

    @Test(expected = com.qadium.cc.QadiumException.class)
    public void testEmptyQueueException() throws QadiumException{
        queues.next(1);
    }

    @Test(expected = java.lang.ArrayIndexOutOfBoundsException.class)
    public void testInValidQueueException() throws QadiumException{
        //This queue Number is not tested in the code
        queues.next(-1);
    }

    /* Testing anything about the messages other than what is
    specified in this document is out of scope
     */
    @Test
    public void testNestedNonJson() throws QadiumException{
        String input = "{\"name\": {\"hello\": {\"who\": \"Qadium\"}}}";
        queues.enqueue(input);
        String json =queues.next(4);
        assertEquals("{\"name\":{\"hello\":{\"who\":\"Qadium\"}}}", json);
    }

    @Test
    public void testQueueMapping() throws QadiumException {
        /*
         * If a message contains the key `_special`, send it to queue 0.
         * If a message contains a `hash` field, send it to queue 1.
         * If a message has a value that includes `muidaQ` (`Qadium` in reverse), send it to queue 2.
         * If a message has an integer value, send it to queue 3.
         * Otherwise, send the message to queue 4.
         */

        //Test Queue 0
        String input = "{'_special': 'whois'}";
        queues.enqueue(input);
        String json =queues.next(0);
        JsonObject jsonObj = Utility.getJsonObject(json);
        assertTrue(jsonObj.has("_special"));

        //Test Queue 1
        input = "{'hash': 'hashvalue'}";
        queues.enqueue(input);
        json =queues.next(1);
        jsonObj = Utility.getJsonObject(json);
        assertTrue(jsonObj.has("hash"));

        //Test Queue 2
        input = "{'company': 'Qadium, Inc.', 'agent': '007'}";
        queues.enqueue(input);
        json =queues.next(2);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("company").getAsString(), ".cnI ,muidaQ");

        //Test Queue 3
        input = "{'company': 23}";
        queues.enqueue(input);
        json =queues.next(3);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("company").getAsInt(), -24);


        //Test Queue 1
        input = "{'hashme': 'hashvalue'}";
        queues.enqueue(input);
        json =queues.next(4);
        jsonObj = Utility.getJsonObject(json);
        assertTrue(jsonObj.has("hashme"));

    }


    @Test
    public void testHash() throws QadiumException {
        /*
        This is a issue, it is throwing null pointer Exception.
        It is throwing error at byte[] unhashedField = ((String)(msgMap.get(fieldToHash))).getBytes(StandardCharsets.UTF_8);
        It will be
        byte[] unhashedField = ((String)(fieldToHash)).getBytes(StandardCharsets.UTF_8);
        */
        try {
            String input = "{'_hash': 'abc'}";
            queues.enqueue(input);
            String json = queues.next(4);
            JsonObject jsonObj = Utility.getJsonObject(json);
            System.out.println(json);
            assertTrue(jsonObj.has("hash"));
        } finally {

            System.out.println(" Issue is present with _hash logic");
        }
    }

    @Test
    public void testInteger() throws QadiumException {

        String input = "{'value': 512}";
        queues.enqueue(input);
        String json =queues.next(3);
        JsonObject jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("value").getAsInt(), -513);

        input = "{'value': 0}";
        queues.enqueue(input);
        json =queues.next(3);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("value").getAsInt(), -1);


        input = "{'value': -2}";
        queues.enqueue(input);
        json =queues.next(3);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("value").getAsInt(), 1);


        input = "{'value': " + String.valueOf(Integer.MAX_VALUE) + "}";
        queues.enqueue(input);
        json =queues.next(3);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("value").getAsInt(), -2147483648);

        input = "{'value': 0.34}";
        queues.enqueue(input);
        json =queues.next(4);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("value").getAsDouble(), 0.34, 0);
    }


    @Test
    public void testPrivate() throws QadiumException {

        try {
            String input = "{'_value': 512}";
            queues.enqueue(input);
            String json = queues.next(4);
            JsonObject jsonObj = Utility.getJsonObject(json);
            //Issue
            assertEquals(jsonObj.get("_value").getAsInt(), 512);

            input = "{'_company': 'Qadium, Inc.', 'agent': '007'}";
            queues.enqueue(input);
            json = queues.next(4);
            jsonObj = Utility.getJsonObject(json);
            assertEquals(jsonObj.get("_company").getAsString(), "Qadium, Inc.");

            input = "{'_company': 'Qadium, Inc.', 'agent': 7}";
            queues.enqueue(input);
            json = queues.next(3);
            jsonObj = Utility.getJsonObject(json);
            assertEquals(jsonObj.get("_company").getAsString(), "Qadium, Inc.");
            assertEquals(jsonObj.get("agent").getAsInt(), -8);

        } finally {
            System.out.println(" Private variable has changed There is Issue");
        }
    }

    @Test(expected = com.qadium.cc.QadiumException.class)
    public void testUpperCase() throws QadiumException {
        String input = "{'company': 'QADium, Inc.', 'agent': '007'}";
        queues.enqueue(input);
        queues.next(2);
        input = "{'HASH': 'hashvalue'}";
        queues.enqueue(input);
        queues.next(1);

        queues.next(4);
        queues.next(4);
    }

    @Test
    public void testQueueMixMapping() throws QadiumException {
        /*
         These rules must be applied in order;
         the first rule that matches is the one you should use.
         */

        String input = "{'_special': 'whois', 'hash': 'hashvalue'}";
        queues.enqueue(input);
        String json =queues.next(0);
        JsonObject jsonObj = Utility.getJsonObject(json);
        assertTrue(jsonObj.has("_special"));
        assertTrue(jsonObj.has("hash"));


         input = "{'hash': 'hashvalue', '_special': 'whois'}";
         queues.enqueue(input);
         json = queues.next(0);
         jsonObj = Utility.getJsonObject(json);
         assertTrue(jsonObj.has("_special"));
         assertTrue(jsonObj.has("hash"));

        input = "{'company': 'Qadium, Inc.', 'agent': 7}";
        queues.enqueue(input);
        json =queues.next(2);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("company").getAsString(), ".cnI ,muidaQ");
        assertEquals(jsonObj.get("agent").getAsInt(), -8);
    }


    @Test
    public void testQueueOrder() throws QadiumException {
        /*
         These rules must be applied in order;
         the first rule that matches is the one you should use.
         */

        String input = "{'_special': 'whois1'}";
        queues.enqueue(input);
        input = "{'_special': 'whois2'}";
        queues.enqueue(input);
        input = "{'_special': 'whois3'}";
        queues.enqueue(input);
        input = "{'_special': 'whois4'}";
        queues.enqueue(input);

        String json =queues.next(0);
        JsonObject jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("_special").getAsString(), "whois1");

        json =queues.next(0);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("_special").getAsString(), "whois2");

        json =queues.next(0);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("_special").getAsString(), "whois3");

        json =queues.next(0);
        jsonObj = Utility.getJsonObject(json);
        assertEquals(jsonObj.get("_special").getAsString(), "whois4");

        exception.expect(com.qadium.cc.QadiumException.class);
        json = queues.next(0);
    }

    @Test
    public void testQueueSequence() throws QadiumException {
        /*
         The sequence implmentation does not work.

         */

        String input = "{'me': 'whois1', '_sequence' : 'seq1', '_part': 1}";
        queues.enqueue(input);
    }


}
