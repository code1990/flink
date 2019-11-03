package com.base;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.List;

public class BaseMongo {
    protected static MongoClient mongoClient ;
		
		static {
			List<ServerAddress> addresses = new ArrayList<ServerAddress>();
			String[] addressList = new String[]{"192.168.80.134"};
			String[] portList = new String[]{"27017"};
			for (int i = 0; i < addressList.length; i++) {
				ServerAddress address = new ServerAddress(addressList[i], Integer.parseInt(portList[i]));
				addresses.add(address);
				
			}
			mongoClient = new MongoClient(addresses);
		}
	
}
