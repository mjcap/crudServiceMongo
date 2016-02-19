package com.capbpm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

//import javax.persistence.Column;
//import javax.persistence.Table;
import javax.xml.bind.JAXBException;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;


@RestController
public class AccessDbController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    private static int upper = 100;
    private static int lower = 1;
    
    @RequestMapping(value="/mapXsdToDb", method = RequestMethod.POST)
    public String mapXsdToDb(@RequestParam(value="input") String input) throws JSONException
    {
    
    	String tableName = null;
    	HashMap<String, ArrayList<Column>> tableNameAndColumns = new HashMap<String, ArrayList<Column>>();
    	ArrayList<Column> columnArrayList = new ArrayList<Column>();
    	HashMap<String, JSONArray> mappingHash = new HashMap<String, JSONArray>();
    	JSONArray mapping = new JSONArray();
    	
    	System.out.println("mapXsdToDb input="+input);
        JSONObject jsonObject = new JSONObject(input);
        System.out.println("mapXsdToDb jsonObject="+jsonObject.toString(2));
        JSONArray jsonArray = jsonObject.getJSONArray("mapping");
        System.out.println("jsonArray="+jsonArray.toString((2)));
        for (int idx=0; idx < jsonArray.length(); idx++){
        	JSONArray subArray = jsonArray.getJSONArray(idx);
        	
        	//{"xsdName":"Account","columnName":"accountNumber","columnType":"xs:string","isKey":true}
            JSONObject jo1 = subArray.getJSONObject(0);
            
            //{"tableName":"account","columnName":"accountnumber","columnType":"UTF8Type"}
            JSONObject jo2 = subArray.getJSONObject(1);
            if (tableName == null){
            	tableName = jo2.getString("tableName");
            	mapping = new JSONArray();
            }
            else{
	            if (!tableName.equals(jo2.getString("tableName"))){
	            	tableNameAndColumns.put(tableName, columnArrayList);
	            	mappingHash.put(tableName, mapping);
	            	columnArrayList = new ArrayList<Column>();
	            	tableName = jo2.getString("tableName");
	            	mapping = new JSONArray();
	            }
            }
            Column c = new Column(jo2.getString("columnName"), jo2.getString("columnType"));
            if (jo1.getBoolean("isKey")){
            	c.setKey(true);
            }
            columnArrayList.add(c);
            System.out.println(jo1+" will map to "+jo2);
            mapping.put(jo1+"|"+jo2);
        }
        tableNameAndColumns.put(tableName, columnArrayList);
    	mappingHash.put(tableName, mapping);
    	

    	 
    	 /*Hashtable<String, Hashtable<String, Object>> tableNameBlankRecordValues = generateCreateStrings(tableNameAndColumns);
 		 
 		 Iterator<String> i = tableNameBlankRecordValues.keySet().iterator();
	     MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
	     MongoClient client = new MongoClient(uri);
	        
 		 System.out.println("creating");
 		 while (i.hasNext()){
 			String indexName = i.next();
 			System.out.println("table="+indexName);

	 			Hashtable<String, Object> blankValueHash = tableNameBlankRecordValues.get(indexName);
	 			Iterator<String> i2 = blankValueHash.keySet().iterator();  
	 			//while (i2.hasNext()){
	 			//	String columnName = i2.next();
	 			//	System.out.println("  columnName:"+columnName+" columnValue:["+blankValueHash.get(columnName)+"]");
	 			//}
	
		        DB db = client.getDB(uri.getDatabase());
		        DBCollection collectionHandle = db.getCollection(indexName);
		        collectionHandle.insert(generateBlankRecordDbObject(blankValueHash));
 			
 		 }*/
    	 
    	//AccessDb ac = new AccessDb();
	    MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
	    MongoClient client = new MongoClient(uri);    	
    	Iterator i = mappingHash.keySet().iterator();
    	while (i.hasNext()){
    		String mappingKey = (String)i.next();
    		System.out.println("mappingKey="+mappingKey+" value:");
    		System.out.println(mappingHash.get(mappingKey).toString(2));
        	
    		//ac.run("insert into mapping (tableName, xsdtodbmappings) values ('"+
    		//       mappingKey +"','" + mappingHash.get(mappingKey).toString()+"')");
    	}
        ArrayList<String> createStrings = generateCreateStrings2(tableNameAndColumns);
       
        JSONObject joResult = new JSONObject();
        
        for (String cString:createStrings){
 		   System.out.println("cString="+cString);
  	       
  	       if (ac.createTable(cString)){
  	          joResult = new JSONObject();
 			  joResult.put("status", "success");
  	       }
  	       else{
   	          joResult = new JSONObject();
  			  joResult.put("status", "error"); 	    	   
  	       }
        }
		

    	return joResult.toString();
    }    

    @RequestMapping(value="/generateTableTemplate", method = RequestMethod.POST)
    public String generateTableTemplate(@RequestParam(value="input") String input) throws JAXBException, JSONException{
         String result = "";
         String mainTableName = null;
         String tableName = null;
         boolean mapFlag = false, mapEntryFlag = false;
         boolean done = false;
         JSONObject jsonResult = new JSONObject();
         jsonResult.put("status", "error");
         
         ArrayList<String> tableNameArrList = new ArrayList<String>();      
         HashMap<String,ArrayList<Column>> tableNameAndColumns = new HashMap<String, ArrayList<Column>>();

     	 RestTemplate restTemplate = new RestTemplate(); 
     	 String s = restTemplate.getForObject(input, String.class);
 	     String[] xsdArray = s.split("\\r?\\n");
 	     Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();    
 	     for (String line : xsdArray){
 	    	line = line.trim();
 	    	line = line.replace("<", "");
 	    	line = line.replace(">", "");
 	    	
 	    	System.out.println("line=["+line+"]");
 	    	String[] lineStrArr = line.split(" ");
 	    	for (String element : lineStrArr){
 	    		//this is the START of the xs:complexType tag and indicates we have a table
 	    		if (element.equals("xs:complexType")){
            		String complexTypeName=findValue(lineStrArr,"name=");
            		if (complexTypeName.equals("Map")){
            			mapFlag = true;
            			//done = false;
            			//mapFlag = false;
            			mapEntryFlag = false;
            			//tableName = mainTableName+"Map";
            			//tableNameArrList.add(tableName);
        			    //tableNameAndColumns.put(tableName, new ArrayList<Column>());
        			    
            			mapEntryFlag = false;
            		}
            		else if (complexTypeName.equals("MapEntry")){
            			        done = false;
            			        mapFlag = false;
            			        mapEntryFlag = true;
	            				tableName = mainTableName+"MapEntry";
	            			    tableNameArrList.add(tableName);
	            			    tableNameAndColumns.put(tableName, new ArrayList<Column>());
	            	}
	            	else{    	
	            		        done = false;
	            		        mapFlag = false;
	            		        mapEntryFlag = false;
	            		        
	                    		mainTableName = complexTypeName;
	                    		tableName = complexTypeName;
	                    		tableNameArrList.add(tableName);
	                    		tableNameAndColumns.put(tableName, new ArrayList<Column>());  
	            	}	            	
            	}
            	else if ((element.equals("xs:element")) && (mapFlag == false) && (done == false)){            		
            		String keyName = findValue(lineStrArr,"name=");
            		String valueType = findValue(lineStrArr,"type=");
            		System.out.println("keyName="+keyName+" valueType="+valueType);
            		if (valueType.equals("tns:Map")){
            		  keyName = keyName+tableName+"MapEntry";
            		  valueType = "xs:string";	
            		}
            		else if (valueType.equals("xs:anyType")){
            		  valueType = "xs:string";
            		}
            		else if (valueType.equals("tns:MapEntry")){
            			valueType = "xs:string";
            		}
            		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
            		keyNameValueTypePairs.put(keyName, valueType);
            		
            		Column col = new Column(keyName, valueType);
            		ArrayList columnArrList = tableNameAndColumns.get(tableName);
            		if (columnArrList.size() == 0){
            			col.setKey(true);
            		}
            		columnArrList.add(col);
            		tableNameAndColumns.put(tableName,columnArrList);
            	} 	
 	    		//this is the END of the xs:complexType tag and indicates the end of this table's definition
            	else if (element.equals("/xs:complexType")){
            		done = true;
            	}
 	    	} 	    	
 	     }
	     
 	     jsonResult = new JSONObject();
         Iterator i = tableNameAndColumns.keySet().iterator();
         while (i.hasNext()){
        	 String tabName = (String) i.next();
        	 ArrayList<Column> columnArrList = tableNameAndColumns.get(tabName);
        	 
        	 JSONArray ja = new JSONArray();
        	 for(Column c:columnArrList){
        		 System.out.println("adding "+c.toJSONMongoObjectString());
        		 ja.put(new JSONObject(c.toJSONMongoObjectString()));
        	 }
        	 jsonResult.put(tabName, ja);
         }
         
 	     System.out.println("generateTableTemplate jsonResult="+jsonResult.toString(2));
         return jsonResult.toString();
    } 
    
    public ArrayList<String> generateCreateStrings2(HashMap<String, ArrayList<Column>> tableNameAndColumns){
    	ArrayList<String> result = new ArrayList();
    	String createString;
    	String tableName;
    	
    	Iterator<String> i = tableNameAndColumns.keySet().iterator();
    	while (i.hasNext()){
    	   tableName = i.next();
    	   createString = "CREATE TABLE "+tableName + "(";
    	   System.out.println(createString);
    	   ArrayList<Column> columns = tableNameAndColumns.get(tableName);
    	   String primaryKey = null;
    	   
    	   for (Column col: columns){
    		   String colName = col.getColumnName();
    		   String colType = col.getColumnType();
    		   String type = null;
    		   boolean isPrimaryKey = col.isKey();
    		   
    		   if (!createString.equals("CREATE TABLE "+tableName + "(")){
    			   createString = createString + ", ";
    	    	   System.out.println(createString);
    		   }
    		   
    		   type = colType;
    		      	    	   
   	    	   createString = createString + colName + " " + type;
   	    	   System.out.println(createString);
   	    	   
   	    	   if (col.isKey()){
   	    		   primaryKey = col.getColumnName();
   	    	   }
    	   }
    	   
    	   if (primaryKey != null){
    	       createString = createString  + ", PRIMARY KEY (" + primaryKey +"));";
        	   System.out.println(createString);
    	   }
    	   else{
    		   createString = createString + ")";
        	   System.out.println(createString);
    	   }
    	   
    	   result.add(createString);
    	}
    			 
    	for (String s:result){
    		System.out.println("generateCreateStrings2 result contains s="+s);
    	}
    	return result;
    }    

    @RequestMapping(value="/listXsd", method = RequestMethod.POST)
    public String listXsd(@RequestParam(value="input") String input) throws JSONException, JsonProcessingException
    {
    	
        String result = "";
        String mainTableName = null;
        String tableName = null;
        boolean mapFlag = false, mapEntryFlag = false;
        boolean done = false;
        JSONObject jsonResult = new JSONObject();
        jsonResult.put("status", "error");
        
        ArrayList<String> tableNameArrList = new ArrayList<String>();      
        HashMap<String,ArrayList<Column>> tableNameAndColumns = new HashMap<String, ArrayList<Column>>();

    	 RestTemplate restTemplate = new RestTemplate(); 
    	 String s = restTemplate.getForObject(input, String.class);
	     String[] xsdArray = s.split("\\r?\\n");
	     Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();    
	     for (String line : xsdArray){
	    	line = line.trim();
	    	line = line.replace("<", "");
	    	line = line.replace(">", "");
	    	
	    	System.out.println("line=["+line+"]");
	    	String[] lineStrArr = line.split(" ");
	    	for (String element : lineStrArr){
	    		//this is the START of the xs:complexType tag and indicates we have a table
	    		if (element.equals("xs:complexType")){
           		String complexTypeName=findValue(lineStrArr,"name=");
           		if (complexTypeName.equals("Map")){
           			mapFlag = true;
           			mapEntryFlag = false;
           		}
           		else if (complexTypeName.equals("MapEntry")){
           			        done = false;
           			        mapFlag = false;
           			        mapEntryFlag = true;
	            				tableName = mainTableName+"MapEntry";
	            			    tableNameArrList.add(tableName);
	            			    tableNameAndColumns.put(tableName, new ArrayList<Column>());
	            	}
	            	else{    	
	            		        done = false;
	            		        mapFlag = false;
	            		        mapEntryFlag = false;
	            		        
	                    		mainTableName = complexTypeName;
	                    		tableName = complexTypeName;
	                    		tableNameArrList.add(tableName);
	                    		tableNameAndColumns.put(tableName, new ArrayList<Column>());  
	            	}	            	
           	}
           	else if ((element.equals("xs:element")) && (mapFlag == false) && (done == false)){
           		String keyName = findValue(lineStrArr,"name=");
           		String valueType = findValue(lineStrArr,"type=");
           		if (valueType.equals("tns:Map")){
           		  valueType = "xs:string";	
           		}
           		else if (valueType.equals("xs:anyType")){
           		  valueType = "xs:string";
           		}
           		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
           		keyNameValueTypePairs.put(keyName, valueType);
           		
           		Column col = new Column(keyName, valueType);
           		ArrayList columnArrList = tableNameAndColumns.get(tableName);
           		if (columnArrList.size() == 0){
           			col.setKey(true);
           		}
           		columnArrList.add(col);
           		tableNameAndColumns.put(tableName,columnArrList);
           	} 	
	    		//this is the END of the xs:complexType tag and indicates the end of this table's definition
           	else if (element.equals("/xs:complexType")){
           		done = true;
           	}
	    	} 	    	
	     }
	     
	     //HashMap<String, ArrayList<Column>> tableNameAndColumns
	     Iterator i = tableNameAndColumns.keySet().iterator();
	     while (i.hasNext()){
	       String tabName = (String)i.next();
	       System.out.println(tabName+" "+tableNameAndColumns.get(tabName));
	     }
	     
	     ObjectWriter ow = new ObjectMapper().writer();
	     String json = ow.writeValueAsString(tableNameAndColumns);
	     
	     System.out.println("json="+json);
	     
	     return json;
	     
    }
    
    @RequestMapping(value="/generateTable3", method = RequestMethod.POST)
    public String generate(@RequestParam(value="input") String input) throws JAXBException, JSONException{
    	
        String result = "";
        String mainTableName = null;
        String tableName = null;
        boolean mapFlag = false, mapEntryFlag = false;
        boolean done = false;
        JSONObject jsonResult = new JSONObject();
        jsonResult.put("status", "error");
        
        ArrayList<String> tableNameArrList = new ArrayList<String>();      
        HashMap<String,ArrayList<Column>> tableNameAndColumns = new HashMap<String, ArrayList<Column>>();

    	 RestTemplate restTemplate = new RestTemplate(); 
    	 String s = restTemplate.getForObject(input, String.class);
	     String[] xsdArray = s.split("\\r?\\n");
	     Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();    
	     for (String line : xsdArray){
	    	line = line.trim();
	    	line = line.replace("<", "");
	    	line = line.replace(">", "");
	    	
	    	System.out.println("line=["+line+"]");
	    	String[] lineStrArr = line.split(" ");
	    	for (String element : lineStrArr){
	    		//this is the START of the xs:complexType tag and indicates we have a table
	    		if (element.equals("xs:complexType")){
           		String complexTypeName=findValue(lineStrArr,"name=");
           		if (complexTypeName.equals("Map")){
           			mapFlag = true;
           			mapEntryFlag = false;
           		}
           		else if (complexTypeName.equals("MapEntry")){
           			        done = false;
           			        mapFlag = false;
           			        mapEntryFlag = true;
	            				tableName = mainTableName+"mapentry";
	            			    tableNameArrList.add(tableName);
	            			    tableNameAndColumns.put(tableName, new ArrayList<Column>());
	            	}
	            	else{    	
	            		        done = false;
	            		        mapFlag = false;
	            		        mapEntryFlag = false;
	            		        
	                    		mainTableName = complexTypeName.toLowerCase();
	                    		tableName = mainTableName;
	                    		tableNameArrList.add(tableName);
	                    		tableNameAndColumns.put(tableName, new ArrayList<Column>());  
	            	}	            	
           	}
           	else if ((element.equals("xs:element")) && (mapFlag == false) && (done == false)){
           		String keyName = findValue(lineStrArr,"name=");
           		keyName = keyName.toLowerCase();
           		String valueType = findValue(lineStrArr,"type=");
           		if (valueType.equals("tns:Map")){
           		  valueType = "xs:string";	
           		}
           		else if (valueType.equals("xs:anyType")){
           		  valueType = "xs:string";
           		}
           		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
           		keyNameValueTypePairs.put(keyName, valueType);
           		
           		Column col = new Column(keyName, valueType);
           		ArrayList columnArrList = tableNameAndColumns.get(tableName);
           		if (columnArrList.size() == 0){
           			col.setKey(true);
           		}
           		columnArrList.add(col);
           		tableNameAndColumns.put(tableName,columnArrList);
           	} 	
	    		//this is the END of the xs:complexType tag and indicates the end of this table's definition
           	else if (element.equals("/xs:complexType")){
           		done = true;
           	}
	    	} 	    	
	     }
	    
         Hashtable<String, Hashtable<String, Object>> tableNameBlankRecordValues = generateCreateStrings(tableNameAndColumns);
 		 
 		 Iterator<String> i = tableNameBlankRecordValues.keySet().iterator();
	     MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
	     MongoClient client = new MongoClient(uri);
	        
 		 System.out.println("creating");
 		 while (i.hasNext()){
 			String indexName = i.next();
 			System.out.println("table="+indexName);

	 			Hashtable<String, Object> blankValueHash = tableNameBlankRecordValues.get(indexName);
	 			Iterator<String> i2 = blankValueHash.keySet().iterator();  
	 			//while (i2.hasNext()){
	 			//	String columnName = i2.next();
	 			//	System.out.println("  columnName:"+columnName+" columnValue:["+blankValueHash.get(columnName)+"]");
	 			//}
	
		        DB db = client.getDB(uri.getDatabase());
		        DBCollection collectionHandle = db.getCollection(indexName);
		        collectionHandle.insert(generateBlankRecordDbObject(blankValueHash));
 			
 		 }
    	jsonResult = new JSONObject();
    	try {
 		   jsonResult.put("status","success");
 	    } catch (JSONException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 	    }	            
    	return jsonResult.toString();
    }
    
    public Hashtable<String, Hashtable<String, Object>> generateCreateStrings(HashMap<String, ArrayList<Column>> tableNameAndColumns){
    	ArrayList<String> result = new ArrayList();
    	String createString;
    	String tableName;
	    Hashtable<String,Hashtable<String, Object>> tableNameBlankRecordValues = new Hashtable<String,Hashtable<String, Object>>();
	    Hashtable<String, Object> blankRecordValues = null;
	    
    	Iterator<String> i = tableNameAndColumns.keySet().iterator();
    	while (i.hasNext()){
    	   blankRecordValues = new Hashtable<String, Object>();	
    	   tableName = i.next();
    	   System.out.println("generateCreateStrings tableName="+tableName);
    	   ArrayList<Column> columns = tableNameAndColumns.get(tableName);
    	   String primaryKey = null;
    	   for (Column col: columns){
    		   String colName = col.getColumnName();
    		   String colType = col.getColumnType();
    		   String type = null;
    		   boolean isPrimaryKey = col.isKey();
    		   
   	    	   if (colType.equals("xs:string")){
   	    		blankRecordValues.put(colName, "");
	    	   }
	    	   else if (colType.equals("xs:dateTime")){
	    		   blankRecordValues.put(colName, "");
	    	   }
	    	   else if (colType.equals("xs:boolean")){
	    		   blankRecordValues.put(colName, new Boolean(false));
	    	   }
	    	   else if (colType.equals("xs:int")){
	    		   blankRecordValues.put(colName, new Integer(0));
	    	   }
	    	   else if (colType.equals("xs:double")){
	    		   blankRecordValues.put(colName, new Double(0));
	    	   }    		   
   	    	   System.out.println("  colName="+colName+" colValue=["+blankRecordValues.get(colName));
    	   }
    	   tableNameBlankRecordValues.put(tableName, blankRecordValues);
    	}
    			 
    	return tableNameBlankRecordValues;
    }    
    /*
     *     	Settings settings = ImmutableSettings.settingsBuilder()
    	        .put("cluster.name", "elasticsearch").build();
    	Client client = new TransportClient(settings)
    	        .addTransportAddress(new InetSocketTransportAddress("odm.capbpm.com", 9300));

    	//generate table and CRUD
 
    	//no concept of generating a table
    	//this will create and index with field names matching the json doc with "" values
    	client.prepareIndex("customer", "customer")
        .setSource(putJsonDocument("",
                                   "",
                                   "",
                                   "",
                                   "")).execute().actionGet();
    	
    	//Create/insert a record
    	client.prepareIndex("customer", "customer")
        .setSource(putJsonDocument("a",
                                   "b",
                                   "c",
                                   "d",
                                   "e")).execute().actionGet();
    	
    	//Retrieve/select a record
    	searchDocument(client, "customer", "customer", "fld1", "a");	

    	//Update a record
    	ArrayList<String> matchingIdx = searchDocumentForId(client, "customer", "customer", "fld1", "a");
    	for (String s:matchingIdx){
    	   updateDocument(client, "customer", "customer", s, "fld2", "I HAVE BEEN CHANGED");
    	}
    	
    	
    	//Delete a record
    	ArrayList<String> matchingIdx = searchDocumentForId(client, "customer", "customer", "fld1", "a");
    	for (String s:matchingIdx){
    		System.out.println("searchDocumentForIdx returned index="+s);
        	deleteDocument(client, "customer", "customer", s);    		
    	}
     */
    //generates table from xsd
    @RequestMapping(value="/generateTableOrig", method = RequestMethod.POST)
    public String generateOrig(@RequestParam(value="input") String input) throws JAXBException, JSONException{
    	
    	String tableName=null;
    	
    	RestTemplate restTemplate = new RestTemplate();
    	String s = restTemplate.getForObject(input, String.class);
	    System.out.println("GenerateController generate() rest call returned s="+s);   
	    Hashtable<String,Object> blankRecordValues = new Hashtable<String, Object>();
	    
	    String[] xsdArray = s.split("\\r?\\n");
	    Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();
	    
	    for (String line : xsdArray){
	    	
	    	line = line.trim();
	    	line = line.replace("<", "");
	    	line = line.replace(">", "");
	    	String[] lineStrArr = line.split(" ");
            for (String element : lineStrArr){
            	
            	if (element.indexOf("/xs:complexType") != -1){
            		
            		String createTable = "CREATE TABLE "+tableName + "(";
            		String type = null;
            		
            		Enumeration e = keyNameValueTypePairs.keys();
            		boolean execute = true;
            		boolean hasAtLeastOneColumn = false;
            		String primaryKey = null;
            		
            	    while (e.hasMoreElements()){
            	    	String columnNameXsd = (String)e.nextElement();
            	    	type = null;
            	    	String xsdType = keyNameValueTypePairs.get(columnNameXsd);
            	    	if (xsdType.compareTo("xs:string")==0){
            	    	    blankRecordValues.put(columnNameXsd, "");
            	    		type = "String";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:dateTime")==0){
            	    		blankRecordValues.put(columnNameXsd, "");
            	    		type = "String";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:boolean")==0){
            	    		blankRecordValues.put(columnNameXsd, new Boolean(false));
            	    		type = "Boolean";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:int")==0){
            	    		blankRecordValues.put(columnNameXsd, new Integer(0));
            	    		type = "Integer";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:double")==0){
            	    		blankRecordValues.put(columnNameXsd, new Double(0));
            	    		type = "Double";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	
            	    	if (type != null){
            	    	  System.out.println("GenerateController generate() colName="+columnNameXsd+" colType="+type);
            	    	  createTable = createTable + columnNameXsd + " " + type;
            	    	  
            	    	  if (primaryKey == null){
            	    		  primaryKey = columnNameXsd;
            	    	  }
              	    	  if (e.hasMoreElements()){
            	    		createTable = createTable +", ";
            	    	  }           	    	  
            	    	  
            	    	}
            	    	


            	    }
            	    createTable = createTable +", PRIMARY KEY (" + primaryKey +"));";
            	    if (execute && hasAtLeastOneColumn){
            	    	System.out.println("create table "+tableName);
            	    	Iterator<String> i = blankRecordValues.keySet().iterator();
            	    	while (i.hasNext()){
            	    	    String columnName=i.next();
            	    	    Object columnValue = blankRecordValues.get(columnName);
            	    	    System.out.println("columnName="+columnName+" columnValue=["+columnValue+"]");
            	    	}
            	        //MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
            	        //MongoClient client = new MongoClient(uri);
            	        //DB db = client.getDB(uri.getDatabase());
            	        //DBCollection collectionHandle = db.getCollection(tableName);
            	        //collectionHandle.insert(generateBlankRecordDbObject(blankRecordValues));
            	       
            	    }
            	    keyNameValueTypePairs = new Hashtable<String,String>();
            	}
            	else if (element.indexOf("xs:complexType") != -1){
            		tableName=findValue(lineStrArr,"name=");
            	}
            	else if (element.indexOf("xs:element") != -1){
            		String keyName = findValue(lineStrArr,"name=");
            		String valueType = findValue(lineStrArr,"type=");
            		keyNameValueTypePairs.put(keyName, valueType);
            	}
            }
	    }
	    
	    
    	JSONObject jsonResult = new JSONObject();
    	try {
 		   jsonResult.put("status","success");
 	    } catch (JSONException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 	    }	            
    	return jsonResult.toString();
    }

    @RequestMapping(value="/create", method = RequestMethod.POST)
    public String create(@RequestParam(value="input") String input) throws JSONException {
    	
		JSONObject jo = new JSONObject(input);
		String table = jo.getString("table");    	
    	
		/*Settings settings = ImmutableSettings.settingsBuilder()
    	        .put("cluster.name", "elasticsearch").build();
    	Client client = new TransportClient(settings)
    	        .addTransportAddress(new InetSocketTransportAddress("odm.capbpm.com", 9300));    */	
		HashMap<String, String> tableColumnNamesTypes = getMetadata(table);
System.out.println("tableColumnNamesTypes="+tableColumnNamesTypes);
		HashMap<String,Object> insertMap = new HashMap<String,Object>();
		
		JSONArray columnNameValueArr = jo.getJSONArray("colval");
		for (int idx=0; idx < columnNameValueArr.length(); idx++){
			JSONObject colValObj = columnNameValueArr.getJSONObject(idx);
			String column = colValObj.getString("column");
			String type = tableColumnNamesTypes.get(column);
			
			
			/*if (type.compareTo("string") == 0){
				SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
				String val = colValObj.getString("value");
				insertMap.put(column, val);
			}
			else if (type.compareTo("double") == 0){
				insertMap.put(column, colValObj.getDouble("value"));
			}else if (type.compareTo("long") == 0){
				insertMap.put(column, colValObj.getLong("value"));
			}else if (type.compareTo("string") == 0){
				insertMap.put(column, colValObj.getString("value"));
			}
			else if (type.compareTo("boolean") == 0){
				insertMap.put(column, colValObj.getBoolean("value"));
			}*/
			
			
			/*
			 *  if (o instanceof String){
                	columnNameAndType.put(columnName, "string");
                }else if (o instanceof Boolean){
                	columnNameAndType.put(columnName, "boolean");
                }
                else if (o instanceof Double){
                	columnNameAndType.put(columnName, "double");
                }
                else if (o instanceof Integer){
                	columnNameAndType.put(columnName, "integer");
                }
                else if (o instanceof Date){;
                	columnNameAndType.put(columnName, "date");
                }
			 */
			if (type.compareTo("string") == 0){
				//SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
				String val = colValObj.getString("value");
				insertMap.put(column, val);
			}else if (type.compareTo("double") == 0){
				insertMap.put(column, colValObj.getDouble("value"));
			}else if (type.compareTo("integer") == 0){
				insertMap.put(column, new Integer(colValObj.getInt("value")));
			}else if (type.compareTo("boolean") == 0){
				insertMap.put(column, colValObj.getBoolean("value"));
			}
			
			
		}
		
		System.out.println("insertMap="+insertMap);
		        MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
		        MongoClient client = new MongoClient(uri);
		        DB db = client.getDB(uri.getDatabase());  	
		        DBCollection customer = db.getCollection(table); 
		        customer.insert(this.generateDbObject(insertMap));
		        client.close();
    	//client.prepareIndex(table, table).setSource(insertMap).execute().actionGet();
    	
    	JSONObject jsonResult = new JSONObject();
    	try {
 		   jsonResult.put("status","success");
 	    } catch (JSONException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 	    }	            
    	return jsonResult.toString();    	
    }

    @RequestMapping(value="/read", method = RequestMethod.POST)
    public String here(@RequestParam(value="input") String input) throws JSONException{
    	
    	JSONArray ja = new JSONArray();
    	JSONObject jo = new JSONObject(input);
		String table = jo.getString("table");
		HashMap<String, String> tableColumnNamesTypes = getMetadata(table);
		JSONArray compArr = jo.getJSONArray("comps");
		
    	RestTemplate restTemplate = new RestTemplate();
    	
    	//this is an equality search
    	//String a = "http://odm.capbpm.com:9200/customer/customer/_search?q=age:32";
    	//this is a range search on numbers
    	//String a = "http://odm.capbpm.com:9200/customer/customer/_search?q={q}";
        MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
        MongoClient client = new MongoClient(uri);
        DB db = client.getDB(uri.getDatabase());  	
        DBCollection customer = db.getCollection(table);
    	    	
    	BasicDBObject findQuery = generateQuery(table, compArr, tableColumnNamesTypes);
        DBCursor docs  = db.getCollection("Customer").find();	        
        docs = customer.find(findQuery);	
        
        JSONArray resultArr = new JSONArray();
        
        while(docs.hasNext()){
            DBObject doc = docs.next();
            if ((doc.get("_id") instanceof String) && (((String)doc.get("_id")).equals("0"))){
            	System.out.println("SKIPPPING "+doc.toString());
            }
            else{
                System.out.println("rec: "+doc.toString());
                Iterator<String> docKeySetIterator = doc.keySet().iterator();
                JSONObject resultObject = new JSONObject();
                while (docKeySetIterator.hasNext()){
                	String column = docKeySetIterator.next();
                	if (!column.equals("_id")){
                		resultObject.put(column,doc.get(column));
                	}
                	
                }
                resultArr.put(resultObject);
            }
        }
        
    	return resultArr.toString();

    }

    @RequestMapping(value="/delete", method = RequestMethod.POST)
    public String delete(@RequestParam(value="input") String input) throws JSONException{
    	
    	JSONArray ja = new JSONArray();
    	JSONObject jo = new JSONObject(input);
		String table = jo.getString("table");
		HashMap<String, String> tableColumnNamesTypes = getMetadata(table);
		JSONArray compArr = jo.getJSONArray("comps");
		
    	RestTemplate restTemplate = new RestTemplate();
    	
    	//this is an equality search
    	//String a = "http://odm.capbpm.com:9200/customer/customer/_search?q=age:32";
    	//this is a range search on numbers
    	//String a = "http://odm.capbpm.com:9200/customer/customer/_search?q={q}";
        MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
        MongoClient client = new MongoClient(uri);
        DB db = client.getDB(uri.getDatabase());  	
        DBCollection customer = db.getCollection(table);
    	    	
    	BasicDBObject findQuery = generateQuery(table, compArr, tableColumnNamesTypes);
        DBCursor docs  = db.getCollection(table).find();	        
        docs = customer.find(findQuery);	
        
        JSONArray resultArr = new JSONArray();
        
        while(docs.hasNext()){
            DBObject doc = docs.next();
            if ((doc.get("_id") instanceof String) && (((String)doc.get("_id")).equals("0"))){
            	System.out.println("SKIPPPING "+doc.toString());
            }
            else{
            	customer.remove(doc);
            }
        }
        
    	JSONObject jsonResult = new JSONObject();
    	try {
 		   jsonResult.put("status","success");
 	    } catch (JSONException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 	    }	            
    	return jsonResult.toString(); 

    }
    
    @RequestMapping(value="/update",method = RequestMethod.POST)
    public String update(@RequestParam(value="input") String input) throws JSONException{
    	
    	JSONArray ja = new JSONArray();
    	JSONObject jo = new JSONObject(input);
		String table = jo.getString("table");
		HashMap<String, String> tableColumnNamesTypes = getMetadata(table);
		JSONArray compArr = jo.getJSONArray("comps");
		JSONArray updateColumnArr = jo.getJSONArray("colval");

        MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
        MongoClient client = new MongoClient(uri);
        DB db = client.getDB(uri.getDatabase());  	
        DBCollection customer = db.getCollection(table);
        
    	BasicDBObject findQuery = generateQuery(table, compArr, tableColumnNamesTypes);
        DBCursor docs  = db.getCollection(table).find();	        
        docs = customer.find(findQuery);        

        while(docs.hasNext()){
            DBObject doc = docs.next();
            if ((doc.get("_id") instanceof String) && (((String)doc.get("_id")).equals("0"))){
            	System.out.println("SKIPPPING "+doc.toString());
            }
            else{
            	//load up fields
            	for (int idx2=0; idx2 < updateColumnArr.length(); idx2++){
            		JSONObject columnNameNewValue = updateColumnArr.getJSONObject(idx2);
            		
            		String columnName = columnNameNewValue.getString("column");
                    String type = tableColumnNamesTypes.get(columnName);
                    Object newValue = null;
                    
                    if (type.equals("string")){
                  	  newValue = columnNameNewValue.getString("value");
                    }
                    else if (type.equals("boolean")){
                  	  newValue = new Boolean(columnNameNewValue.getString("value"));
                  	  
                    }else if (type.equals("double")){
                  	  newValue = new Double(columnNameNewValue.getString("value"));
                  	  
                    }else if (type.equals("integer")){
                  	  newValue = new Integer(columnNameNewValue.getString("value"));
                    }else if (type.equals("date")){
                  	  newValue = new Date(columnNameNewValue.getString("value"));
                    }
                    
            		doc.put(columnName, newValue);
            	}            	
            	customer.update(new BasicDBObject("_id", doc.get("_id")),doc); 
            }
        }
                
    	JSONObject jsonResult = new JSONObject();
    	try {
 		   jsonResult.put("status","success");
 	    } catch (JSONException e2) {
 			// TODO Auto-generated catch block
 			e2.printStackTrace();
 	    }	            
    	return jsonResult.toString(); 

    }
    
    public HashMap<String, String> generateQueryString(JSONArray compArr, HashMap<String, String> tableColumnNamesTypes) throws JSONException{
    	HashMap<String, String> queryMap = new HashMap<String, String>();
    	String comparisonClause = "-_id:0 AND (";
    	
    	/*
    	 * comparisons ==, <=, >=, <, >, !=
    	 * ONLY SUPPORTS RANGE SEARCHES SO <, <= must be of form 
    	 * 
    	 * exclusive minValue < startDate < maxValue
    	 * {"column":"startDate" , "comp":"between", "minValue":"2015-11-23 12:00:00", "maxValue":"xxxxxxxxxxxx" }
    	 * 
    	 * or 
    	 * 
    	 * inclusive minValue <= startDate <= maxValue
    	 * 
    	 * if =  columnName:value                       {"column":"columnName" , "comp":"=", "value":"value" }
    	 * if != -columnName:value                      {"column":"columnName" , "comp":"!=", "value":"value" }
    	 * if <, <= MUST provide minValue & maxValue   
    	 *      < columnName:{minValue to maxValue}     {"column":"columnName" , "comp":"<", "value1":"value", "value2":"value" }
    	 *      <= columnName:[minValue to maxValue]    {"column":"columnName" , "comp":"<=", "value1":"value", "value2":"value" }
    	 */
		//JSONArray compArr = jo.getJSONArray("comps");
		for (int idx=0; idx < compArr.length(); idx++){
			ArrayList<Object> valuesList = new ArrayList<Object>();
			
			JSONObject compObj = compArr.getJSONObject(idx);
			String columnName = compObj.getString("column");
			String comp = compObj.getString("comp");
			if ((comp.equals("<") || comp.equals("<="))){
			   valuesList.add(compObj.getString("value1"));
			   valuesList.add(compObj.getString("value2"));
			}
			else{
				valuesList.add(compObj.getString("value"));;
			}
			
            String type = tableColumnNamesTypes.get(columnName);
			if (type.compareTo("string") == 0){
				//comparisonClause = comparisonClause + key + " " + comp + " " + parsedDate.getTime();
				comparisonClause = comparisonClause + generateLuceneComp(columnName, type, comp, valuesList);
			}
			else if (type.compareTo("double") == 0){
				//comparisonClause = comparisonClause + key + " " + comp + " " + new Double(val).doubleValue();
				comparisonClause = comparisonClause + generateLuceneComp(columnName, type, comp, valuesList);
			}else if (type.compareTo("long") == 0){
				//comparisonClause = comparisonClause + key + " " + comp + " " + new Integer(val).intValue();
				comparisonClause = comparisonClause + generateLuceneComp(columnName, type, comp, valuesList);
			}else if (type.compareTo("boolean") == 0){
			    //comparisonClause = comparisonClause + "=" + val;
				comparisonClause = comparisonClause + generateLuceneComp(columnName, type, comp, valuesList);
			}
			
			comparisonClause = comparisonClause + ")";
			
		}
		queryMap.put("q",comparisonClause);
    	return queryMap;
    }
    
    public String generateLuceneComp(String fieldName, String fieldType, String compType, ArrayList<Object>valuesList){
    	String result = "";
    	
    	
    	if (compType.compareTo("<=") == 0){
    	   if (fieldType.equals("string")){
    		   //fieldName:["valueList[0]" TO "valueList[1]"]
    		   result = fieldName + ":[\"" + valuesList.get(0) + "\" TO \"" + valuesList.get(1) + "\"]";  
    	   }
    	   else{
    		   //fieldName:[valueList[0] TO valueList[1]]
    		   result = fieldName + ":[" + valuesList.get(0) + " TO " + valuesList.get(1) + "]";  
    	   }
    	}
    	else if (compType.compareTo("<") == 0){
     	   if (fieldType.equals("string")){
    		   //fieldName:{"valueList[0]" TO "valueList[1]"}
    		   result = fieldName + ":{\"" + valuesList.get(0) + "\" TO \"" + valuesList.get(1) + "\"}";  
    	   }
    	   else{
    		   //fieldName:[valueList[0] TO valueList[1]]
    		   result = fieldName + ":{" + valuesList.get(0) + " TO " + valuesList.get(1) + "}";  
    	   }    		
    	}
    	else if (compType.compareTo("!=") == 0){
    		// -fieldName:valueList[0]
    		if (fieldType.equals("string")){
    		  result = "-" + fieldName + ":\"" + valuesList.get(0) + "\"";
    		}
    		else{
    			result = "-" + fieldName + ":" + valuesList.get(0) ;
    		}
    	}
    	else if (compType.compareTo("=") == 0){
    		// fieldName:valueList[0]
    		if (fieldType.equals("string")){
    		  result = fieldName + ":\"" + valuesList.get(0) + "\"";
    		}
    		else{
    			result = fieldName + ":" + valuesList.get(0) ;
    		}    		
    	}
    	return result;
    }
    	/*@RequestMapping("/read")
    public String read(@RequestParam(value="input") String input) {
    	
    }
    
    public void searchDocument(Client client, String index, String type,
            String field, String value){
			SearchResponse response = client.prepareSearch(index)
			              .setTypes(type)
			              .setSearchType(SearchType.QUERY_AND_FETCH)
			              .setQuery(QueryBuilders.termQuery(field, value))
			              .setFrom(0).setSize(60).setExplain(true)
			              .execute()
			              .actionGet();
			SearchHit[] results = response.getHits().getHits();
			System.out.println("Current results: " + results.length);
			for (SearchHit hit : results) {
				System.out.println("------------------------------");
				Map<String,Object> result = hit.getSource();    
				System.out.println(result);
			}
}*/
 /*
    public ArrayList<String> searchDocumentForId(Client client, String index, String type,
            String field, String value){
    	
    	ArrayList<String> result = new ArrayList<String>();
			SearchResponse response = client.prepareSearch(index)
			              .setTypes(type)
			              .setSearchType(SearchType.QUERY_AND_FETCH)
			              .setQuery(QueryBuilders.termQuery(field, value))
			              .setFrom(0).setSize(60).setExplain(true)
			              .execute()
			              .actionGet();
			SearchHit[] results = response.getHits().getHits();
			System.out.println("Current results: " + results.length);
			for (SearchHit hit : results) {
				System.out.println("------------------------------");
				result.add(hit.getId());    
			}
			return result;
}
   */ 
    /*public static Map<String, Object> putJsonDocument(String title, String content, String postDate, 
            String tags, String author){
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("fld1", title);
		jsonDocument.put("fld2", content);
		jsonDocument.put("fld3", postDate);
		jsonDocument.put("fld4", tags);
		jsonDocument.put("fld5", author);
		return jsonDocument;
    } */
    
    public static Map<String, Object> putJsonDocument(String title, String content, String postDate, 
            String tags, String author){
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("fld1", title);
		jsonDocument.put("fld2", content);
		jsonDocument.put("fld3", postDate);
		jsonDocument.put("fld4", tags);
		jsonDocument.put("fld5", author);
		return jsonDocument;
    }    
    

    
 /*   public static void deleteDocument(Client client, String index, String type, String id){
        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        System.out.println("Information on the deleted document:");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
    }
    
    public static void updateDocument(Client client, String index, String type, 
            String id, String field, Object newValue){
		Map<String, Object> updateObject = new HashMap<String, Object>();
		updateObject.put(field, newValue);
		client.prepareUpdate(index, type, id)
		.setScript("ctx._source." + field + "=" + field,ScriptService.ScriptType.INLINE)
		.setScriptParams(updateObject).execute().actionGet();
    }
    
    public HashMap<String, String> getMetadata(String tableName) throws JSONException{

    	HashMap<String,String> columnNameType = new HashMap<String,String>();
    	
    	RestTemplate restTemplate = new RestTemplate();
    	String s = restTemplate.getForObject("http://odm.capbpm.com:9200/customer/_mapping/"+tableName, String.class);
    	JSONObject metadata=new JSONObject(s);
  
    	//metadata.getJSONObject(tableName).getJSONObject("mappings").getJSONObject("properties");
    	JSONObject columnsAndTypesJSONObjects = metadata.getJSONObject(tableName).getJSONObject("mappings").getJSONObject(tableName).getJSONObject("properties");
	    System.out.println("GenerateController generate() rest call returned metadata=");
	    Iterator i = columnsAndTypesJSONObjects.keys();
	    while (i.hasNext()){
	    	String columnName = (String) i.next();
	    	JSONObject typeObject = columnsAndTypesJSONObjects.getJSONObject(columnName);
	    	String type = typeObject.getString("type");
	    	columnNameType.put(columnName, type);
	    }
	    System.out.println(metadata.getJSONObject(tableName).getJSONObject("mappings").getJSONObject(tableName).getJSONObject("properties").toString(2)); 
	 
	    i = columnNameType.keySet().iterator();
	    while (i.hasNext()){
	    	String cn = (String) i.next();
	    	System.out.println("columnName="+cn+" type="+columnNameType.get(cn));
	    }
	    
	    return columnNameType;
    }*/
    
    //xs:complexType name="Customer"
    public String findValue(String[] arr, String key){
    	//System.out.println("findValue arr="+arr+" key="+key);
    	String value=null;
    	for (int idx = 1; idx < arr.length; idx++){
    		if (arr[idx].indexOf(key) != -1){
    			String[] keyValueArr=arr[idx].split("=");
    			value = keyValueArr[1];
    			value = value.replace("\"", "");
    			break;
    		}
    	}
    	return value;
    	
    } 
    
    //TODO:  deletes all data in collection but does not remove collection
    @RequestMapping(value="/deleteTable", method = RequestMethod.POST)
	public String deleteTable(@RequestParam(value="input") String input) throws JSONException{
    	
    	String tableName = input;
    	System.out.println("in deleteTable() tableName="+tableName);
        MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
        MongoClient client = new MongoClient(uri);
        DB db = client.getDB(uri.getDatabase());
	    DBCollection myCollection = db.getCollection(input);
	    myCollection.drop();
	    
	    JSONObject joResult = new JSONObject();
	    joResult.put("status", "success");
	    return joResult.toString();
    }
    
    public HashMap<String, String> getMetadata(String tableName){
    	System.out.println("getMetadata tableName="+tableName);
    	HashMap<String, String> columnNameAndType = new HashMap<String, String>();
        //MongoClientURI uri  = new MongoClientURI("mongodb://user:pass@host:port/db"); 
        MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
        MongoClient client = new MongoClient(uri);
        DB db = client.getDB(uri.getDatabase());
        
        /*
         * First we'll add a few songs. Nothing is required to create the
         * songs collection; it is created automatically when we insert.
         */
        
        //DBCollection customer = db.getCollection("Customer");   	
        DBCollection customer = db.getCollection(tableName); 
        BasicDBObject findQuery = new BasicDBObject("_id", new BasicDBObject("$eq","0"));
        DBCursor docs = customer.find(findQuery);	        
        while(docs.hasNext()){
            DBObject doc = docs.next();
            Iterator<String> i = doc.keySet().iterator();
            while (i.hasNext()){
            	String columnName = i.next();
                Object o = doc.get(columnName);
                System.out.println(columnName+"="+o.toString());
                if (o instanceof String){
                	columnNameAndType.put(columnName, "string");
                	System.out.println(" STRING");
                }
                else if (o instanceof Boolean){
                	System.out.println(" BOOLEAN");
                	columnNameAndType.put(columnName, "boolean");
                }
                else if (o instanceof Double){
                	System.out.println(" DOUBLE");
                	columnNameAndType.put(columnName, "double");
                }
                else if (o instanceof Integer){
                	System.out.println(" INTEGER");
                	columnNameAndType.put(columnName, "integer");
                }
                else if (o instanceof Date){
                	System.out.println(" DATE");
                	columnNameAndType.put(columnName, "date");
                }
                /*else{
                	System.out.println(" this is not a string");
                }*/
            }
        }
        client.close();
        return columnNameAndType;
        
    }
    
    public BasicDBObject generateBlankRecordDbObject(Hashtable<String,Object> colNameBlankValue){
        BasicDBObject basicDbObject = new BasicDBObject();
        
        Iterator<String> i = colNameBlankValue.keySet().iterator();
        basicDbObject.put("_id", Integer.toHexString(0));
        while (i.hasNext()){
        	String columnName = i.next();
        	Object value = colNameBlankValue.get(columnName);
            basicDbObject.put(columnName, value);
        }
        
        return basicDbObject; 	
    }
    
    public BasicDBObject generateDbObject(HashMap<String,Object> colNameBlankValue){
        BasicDBObject basicDbObject = new BasicDBObject();
        
        Iterator<String> i = colNameBlankValue.keySet().iterator();
        while (i.hasNext()){
        	String columnName = i.next();
        	Object value = colNameBlankValue.get(columnName);
            basicDbObject.put(columnName, value);
        }
        
        return basicDbObject; 	
    }
    
    public BasicDBObject generateQuery(String tableName, JSONArray compArr, HashMap<String, String> tableColumnNamesTypes) throws JSONException{
    	BasicDBObject query=null;;
    	System.out.println("generateQuery for "+tableName);
    	System.out.println("compArr="+compArr.toString(2));
    	System.out.println("tableColumnNamesTypes="+tableColumnNamesTypes);
        MongoClientURI uri  = new MongoClientURI("mongodb://testUser:12345678@odm.capbpm.com:27017/test");
        MongoClient client = new MongoClient(uri);
        DB db = client.getDB(uri.getDatabase());   	
        DBCollection customer = db.getCollection(tableName); 
        /*compArr=[{
  	                 "column": "startDate",
  	                 "comp": "=",
  	                 "value": "2015-11-23 12:00:00"
  	                }
  	              ]*/         
        for (int idx=0; idx<compArr.length(); idx++){
            /*compObj={
              "column": "startDate",
              "comp": "=",
              "value": "2015-11-23 12:00:00"
             }*/        	
          JSONObject compObj = compArr.getJSONObject(idx);
          
          String column = compObj.getString("column");
          String comparison = compObj.getString("comp");
          String comparisonName = "";
          String type = tableColumnNamesTypes.get(column);
          /*                if (o instanceof String){
      	columnNameAndType.put(columnName, "string");
      	System.out.println(" STRING");
      }
      else if (o instanceof Boolean){
      	System.out.println(" BOOLEAN");
      	columnNameAndType.put(columnName, "boolean");
      }
      else if (o instanceof Double){
      	System.out.println(" DOUBLE");
      	columnNameAndType.put(columnName, "double");
      }
      else if (o instanceof Integer){
      	System.out.println(" INTEGER");
      	columnNameAndType.put(columnName, "integer");
      }
      else if (o instanceof Date){
      	System.out.println(" DATE");
      	columnNameAndType.put(columnName, "date");
      }
*/        Object value=null;  
          if (type.equals("string")){
        	  value = (String)compObj.get("value");
          }
          else if (type.equals("boolean")){
        	  value = new Boolean((String)compObj.get("value"));
        	  
          }else if (type.equals("double")){
        	  value = new Double((String)compObj.get("value"));
        	  
          }else if (type.equals("integer")){
        	  value = new Integer((String)compObj.get("value"));
          }else if (type.equals("date")){
        	  value = new Date((String)compObj.get("value"));
          }
          
          if (comparison.equals("<")){
        	  comparisonName = "$lt";
          }
          else if (comparison.equals(">")){
        	  comparisonName = "$gt";
          }
          else if (comparison.equals("=")){
        	  comparisonName = "$eq";
          }
          System.out.println(column+" "+comparisonName+" "+value);
          query = new BasicDBObject(column, new BasicDBObject(comparisonName,value));
        }
        client.close();
        
        return query;
        //return new BasicDBObject();
    }
}