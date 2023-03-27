package com.renfer.changestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class ChangeStreamsApplicationTests {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void runMongoDBDocumentProducer() {
        MongoDBDocumentProducer bean = applicationContext.getBean(MongoDBDocumentProducer.class);
        assertNotNull(bean);
        bean.run(1000);
    }
    
    @Test
    public void watchInParallel() throws InterruptedException, ExecutionException {
    	int numberOfThreads = 5;
    	var threadPool = Executors.newFixedThreadPool(numberOfThreads);
    	
    	List<Callable<String>> tasks = new ArrayList<>();
    	for(int i=0; i< numberOfThreads; i++) {
    		var watch = applicationContext.getBean(MongoDBWatch.class);
    		watch.setTotalPartitionCount(numberOfThreads);
    		watch.setPartitionIndex(i);
    		tasks.add(watch);
    	}
        
        var futures = threadPool.invokeAll(tasks);
        for(Future<String> future : futures) {
        	assertEquals(future.get(),"");
        }
    }
}
