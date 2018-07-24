package com.iptime.iothome.mongodb;

import static org.junit.Assert.*;

import java.net.UnknownHostException; 
import java.util.Set;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Categories.ExcludeCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class MongoDbTest {
	
	private Logger logger = LoggerFactory.getLogger(MongoDbTest.class);

	private MongoClient client;
	private MongoDatabase db;
	
	@Before
	public void setUp() throws UnknownHostException{
		client =  new MongoClient();	//default : localhost : 27017
		db = client.getDatabase("iot");
	}
	
	@Test
	public void test() throws UnknownHostException {
		/***given***/
		
		/***when***/
		MongoIterable<String> collections = db.listCollectionNames();
		logger.debug("{}", collections.toString());
		
		MongoCollection<Document> dbc = db.getCollection("iot");
		logger.debug("{}", dbc.count());
		
		/***then***/
		for(String str : collections)
			logger.debug(str);
	}
	
	@Test
	public void insertTest() throws UnknownHostException{
		/***Given***/
		MongoIterable<String> collections = db.listCollectionNames();
		logger.debug("{}", collections.toString());
		
		MongoCollection<DBObject> dbc = db.getCollection("iot",DBObject.class);
		logger.debug("{}", dbc.count());
		
		/***When***/
		//emptry object(document) 생성
		BasicDBObjectBuilder docBuilder = BasicDBObjectBuilder.start();
		//docBuilder.append("_id", "dev1_" + System.currentTimeMillis());
		docBuilder.append("dev", "dev1");
		docBuilder.append("timestamp", System.currentTimeMillis());
		docBuilder.append("value", 321);
		
		//docBuilder.
		DBObject dbObject = docBuilder.get();
		
		try{
			dbc.insertOne(dbObject);
		}catch(MongoWriteException e){
			fail("MongoWriteException");
		}catch(MongoWriteConcernException e){
			fail("MongoWriteConcernException");
		}

	}
}
