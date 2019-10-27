package util;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 13:24
 * @Author: code1990
 * @Description:
 */
public class MongoUtils {

    private static MongoClient mongoClient = new MongoClient("192.168.80.134", 27017);

    public static Document findOneBy(String tableName, String dataBase, String yearBaseType) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dataBase);
        MongoCollection collection = mongoDatabase.getCollection(tableName);
        Document document = new Document();
        FindIterable<Document> iterable = collection.find(document);
        MongoCursor<Document> cursor = iterable.iterator();
        if (cursor.hasNext()) {
            return cursor.next();
        } else {
            return null;
        }
    }

    public static void saveOrUpdateMongo(String tableName, String dataBase, Document document) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dataBase);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(tableName);
        if (!document.containsKey("_id")) {
            ObjectId objectId = new ObjectId();
            document.put("_id", objectId);
            mongoCollection.insertOne(document);
        }
        Document matchDocument = new Document();
        String objectId = document.getString("_id").toString();
        matchDocument.put("_id", new ObjectId(objectId));
        FindIterable<Document> findIterable = mongoCollection.find(matchDocument);
        if (findIterable.iterator().hasNext()) {
            mongoCollection.updateOne(matchDocument, new Document("$set", document));
            System.out.println(JSONObject.toJSONString(document));
        } else {
            mongoCollection.insertOne(document);
            System.out.println(JSONObject.toJSONString(document));
        }

    }

}
