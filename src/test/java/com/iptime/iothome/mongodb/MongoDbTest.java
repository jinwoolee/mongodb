package com.iptime.iothome.mongodb;

import java.net.UnknownHostException;

import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoDbTest {

	@Test
	public void test() throws UnknownHostException {
		/***given***/
		MongoClient client =  new MongoClient();
		DB db = client.getDB("local");
		
		/***when***/

		/***then***/
	}

}
