package com.iptime.iothome.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.UnknownHostException;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Block;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

public class MongoDbTest {
	
	private Logger logger = LoggerFactory.getLogger(MongoDbTest.class);

	private MongoClient client;
	private MongoDatabase db;
	private Document document; 
	
	private final String testCollection = "iot";
	private final String testId = "testId";
	private final String testDev = "testDev";
	private final long testValue = 830325;
	
	@Before
	public void setUp() throws UnknownHostException{
		client =  new MongoClient();	//default : localhost : 27017
		db = client.getDatabase("iot");
		
		//initData();
	}
	
	public void initData() {
		document = new Document();
		document.append("_id", testId);
		document.append("dev", testDev);
		document.append("value", testValue);
		
		MongoCollection<Document> collection = db.getCollection(testCollection);
		
		collection.deleteOne(document);
		collection.insertOne(document);
	}
	
		
	//@Test
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
	
	//@Test
	public void insertTest() throws UnknownHostException{
		/***Given***/
		MongoIterable<String> collections = db.listCollectionNames();
		logger.debug("{}", collections.toString());
		
		MongoCollection<DBObject> dbc = db.getCollection("iot",DBObject.class);
		logger.debug("{}", dbc.count());
		
		/***When***/
		//emptry object(document) 생성
		BasicDBObjectBuilder docBuilder = BasicDBObjectBuilder.start();
		
		String id = "dev1_" + System.currentTimeMillis();
		docBuilder.append("_id", id);
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
		
		/***Then***/
		assertEquals(id, dbObject.get("_id"));
		logger.debug("{}", "id : " + dbObject.get("_id"));
	}
	
	@Test
	public void updateTest() {
		/***given***/
		MongoCollection<Document> collection = db.getCollection(testCollection);
		
		/***when***/
		//collection.replaceOne(document);
		UpdateResult updateResult = collection.updateOne(Filters.eq("_id", testId), Updates.combine(Updates.set("value", 830325 + 1000000)));
		
		//FindIterable<Document> documents = collection.find(new Document().append("_id", testId));
		//Document doc = documents.first();

		/***then***/
		assertEquals(1, updateResult.getModifiedCount());
		//assertEquals(830325 + 2000000, doc.get("value"));		
	}
	
	@Test
	public void readTest() {
		/***given***/
		
		Block<Document> printBlock = new Block<Document>() {
		       //@Override
		       public void apply(final Document document) {
		           System.out.println(document.toJson());
		       }
		};
		
		MongoCollection<Document> collection = db.getCollection(testCollection);
		
		/***when***/
		FindIterable<Document> findIterable = collection.find();
		findIterable.forEach(printBlock);
		
		logger.debug("{}", "===============eq===============");
		//collection.find(Filters.eq("_id", testId)).forEach(printBlock);
		findIterable = collection.find(Filters.eq("_id", testId));
		findIterable = findIterable.projection(new Document().append("dev", null).append("value", null));
		//findIterable.forEach(printBlock);
		
		//여기서부터 
		Document doc = findIterable.first();
		logger.debug("{}", "doc.toString() : " + doc.toJson());
		
		/***then***/
		assertEquals(testId, doc.get("_id"));
		assertEquals(testDev, doc.get("dev"));
	}
}
