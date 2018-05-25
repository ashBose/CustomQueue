package com.qadium.cc;

import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import com.anarsoft.vmlens.concurrent.junit.ConcurrentTestRunner;
import com.anarsoft.vmlens.concurrent.junit.ThreadCount;


@RunWith(ConcurrentTestRunner.class)
public class concurrentQueueTest {

    private QadiumQueues queues = null;
    private final static int THREAD_COUNT = 2;

    @Before
    public void initialize(){
        queues = new QadiumQueues();
    }

    @Test
    @ThreadCount(THREAD_COUNT)
    public void test() throws QadiumException{
        String input = "{'_special': 'whois'}";
        queues.enqueue(input);
        input = "{'hash': 'hashvalue'}";
        queues.enqueue(input);
        input = "{'company': 'Qadium, Inc.', 'agent': '007'}";
        queues.enqueue(input);
        input = "{'company': 23}";
        queues.enqueue(input);
        queues.next(0);
        queues.next(1);
        queues.next(2);
        queues.next(3);
    }

}
