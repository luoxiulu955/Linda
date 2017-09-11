/**
 * Client thread that send requests, including "add", "get id", "update",
 * "update", "out", "in" and "rd", to the servers
 * 
 * @author Liang Xia
 * 
 */
import java.net.*;
import java.util.List;
import java.util.Map;
import java.io.*;

public class Client extends Thread{
	 private String serverName ;
	 private int serverPort;
	 private List<Host> hosts;
	 private String type;
	 private String localCustomizedHost;
	 private Host source;
	 private String datastr;
	 private String originOutTupleText;
	 private boolean hasVariable;
	 private boolean isSearch;
	 private String target; // origin or backup
	 private Server server;
	 private Map<String, String> backupMaps;

	 //in and rd
	 public Client(String serverName, int serverPort, String type, String target,String datastr, boolean hasVariable, boolean isSearch){
		 this.serverName = serverName;
		 this.serverPort = serverPort;
		 this.datastr = datastr;
		 this.type = type;
		 this.hasVariable = hasVariable;
		 this.isSearch = isSearch;
		 this.target = target;
	 }
	 
	 //out
	 public Client(String serverName, int serverPort, String type, String target, String datastr, String originOutTupleText){
		 this.serverName = serverName;
		 this.serverPort = serverPort;
		 this.target = target;
		 this.datastr = datastr;
		 this.type = type;
		 this.originOutTupleText = originOutTupleText;
	 }
	 
	 //syncupdate, delete, update
	 public Client(String serverName, int serverPort, String type, Host source){
		 this.serverName = serverName;
		 this.serverPort = serverPort;
		 this.source = source;
		 this.type = type;
	 }
	 
	 //elect and synchosts
	 public Client(String serverName, int serverPort, String type, Host source, Server server){
		 this.serverName = serverName;
		 this.serverPort = serverPort;
		 this.source = source;
		 this.type = type;
		 this.server = server;
	 }
	 
	 //add
	 public Client(String serverName, int serverPort, List<Host> hosts, String type, String customizedHost){
		 this.serverName = serverName;
		 this.serverPort = serverPort;
		 this.hosts = hosts;
		 this.type = type;
		 localCustomizedHost = customizedHost;
	 }
	 
	 //exit, calculate backup server map
	 public Client(String serverName, int serverPort, String type) {
		 this.serverName = serverName;
		 this.serverPort = serverPort;
		 this.type = type;
	 }
	 
	 public Client(String serverName, int serverPort, String type, String target) {
		 this.serverName = serverName;
		 this.serverPort = serverPort;
		 this.type = type;
		 this.target = target;
	 }
	 
	 //sync tuple
	 public Client(String serverName, int serverPort, String type, String target, Host source) {
		 this.serverName = serverName;
		 this.serverPort = serverPort;
		 this.type = type;
		 this.target = target;
		 this.source = source;
	 }
	 
	 //update backup maps to new hosts
	 public Client (String serverName, int serverPort, String type, Map<String, String> backupMaps) {
		 this.serverName = serverName;
		 this.serverPort = serverPort;
		 this.type = type;
		 this.backupMaps = backupMaps;
	 } 

	 public void run() {
		 try {
			 Socket client = new Socket(serverName, serverPort);
	         OutputStream outToServer = client.getOutputStream();
	         DataOutputStream out = new DataOutputStream(outToServer);
	         InputStream inFromServer = client.getInputStream();
	         DataInputStream in = new DataInputStream(inFromServer);
	         
	         if(type.equals("add")) {
	        	 StringBuilder sb = new StringBuilder();
	        	 int count = hosts.size();
	        	 sb.append(type + " " + count);
	        	 for (Host host : hosts) {
	        		 if(hosts.size() == 1 && localCustomizedHost.equals(host.getHostname())){
	        			 host.setIsMaster(true);
	        		 }
	        		 sb.append(" " + host.getID() + 
	        				   " " + host.getHostname() + 
	        				   " " + host.getIPAddr() + 
	        				   " " + host.getPort() +
	        				   " " + host.getStatus());
	        	 }
	        	 out.writeUTF(sb.toString());
	         } else if(type.equals("delete")) { 
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type + " " + source.getHostname());
	        	 out.writeUTF(sb.toString());
	        	 Boolean.valueOf(in.readUTF()); //get response once delete finish, in order to avoid race condition
	         } else if(type.equals("elect")) {
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type + 
	        			   " " + source.getHostname() +
  					   	   " " + source.getIPAddr() + 
  					   	   " " + source.getPort());
	        	 out.writeUTF(sb.toString());
	        	 String inMessage = in.readUTF();
//	        	 System.out.println("Client: get message from server: " + inMessage);
	        	 System.out.println(Boolean.valueOf(inMessage));
	        	 boolean result = Boolean.valueOf(inMessage);
	        	 server.setElected(result);
//	        	 System.out.println("Client: Get elect result from other node: " + inMessage);
	         } else if (type.equals("getID")) {
	        	 StringBuilder sb = new StringBuilder();
	        	 for (Host host : hosts){
	        		 if (localCustomizedHost.equals(host.getHostname())){
	        			 sb.append(type + 
	        					   " " + host.getHostname() +
		        				   " " + host.getIPAddr() + 
		        				   " " + host.getPort() +
		        				   " " + host.getStatus());
	    	        	 out.writeUTF(sb.toString());
	    		         int newID = Integer.valueOf(in.readUTF());
	    		         host.setID(newID);
	        			 break;
	        		 }
	        	 }	         
	         } else if (type.equals("update")) {
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type +
	        			   " " + source.getID() +
  					   	   " " + source.getHostname() +
  					   	   " " + source.getIPAddr() + 
  					   	   " " + source.getPort() +
  					   	   " " + source.getStatus());
	        	 out.writeUTF(sb.toString());
	         } else if (type.equals("claim")) {
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type + " " + source.getHostname());
	        	 out.writeUTF(sb.toString());
	         } else if (type.equals("synchosts")) { 
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type + 
	        			   " " + source.getHostname() + 
	        			   " " + source.getPort());
	        	 out.writeUTF(sb.toString());
	        	 String inMessage = in.readUTF();
	        	 if (inMessage.equals("refuse")) {
	        		 System.out.println("Refused to sycronize hosts and tuple since it is not existing in the cluster");
	        	 } else {
		        	 String[] inMsgSegs = inMessage.split(" ");
		        	 int count = Integer.valueOf(inMsgSegs[0]);
		        	 Host tmpSelf = null;
	            	 for (Host h : server.getHosts()) { 
	            		 if(h.getHostname().equals(server.getHostname())) {
	            			 tmpSelf = h;
	            		 }
	            	 }
	            	 server.getHosts().clear();
	            	 server.getHosts().add(tmpSelf);
	            	 server.getConsistentHashing().addNode(tmpSelf);
		         	 for(int i = 0; i < count; i++){
		            	 int id = Integer.valueOf(inMsgSegs[1+i*5]);
		             	 String hostname = inMsgSegs[2+i*5];
		            	 String ipAddr = inMsgSegs[3+i*5];
		            	 int port = Integer.valueOf(inMsgSegs[4+i*5]);
		            	 boolean isMaster = Boolean.parseBoolean(inMsgSegs[5+i*5]);
		            	 System.out.println("get " + hostname + " record.");
		            	 if(!server.getHostname().equals(hostname)) {
		            		 Host tmp = new Host(id, hostname, ipAddr, port, isMaster);
			            	 server.getHosts().add(tmp);
			            	 server.getConsistentHashing().addNode(tmp);
			            	 FileHandler.writeHost(tmp, true);
		            	 } else {
		            		 server.getHosts().get(0).setID(id);
		            	 }
		            	 if(isMaster) {
		            		 Host master = new Host(id, hostname, ipAddr, port, isMaster);
		            		 server.setMasterHost(master);
		            	 }
		         	 }
	        	 }
	         } else if (type.equals("syncupdate")) { 
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type + 
	        			   " " + source.getHostname() + 
	        			   " " + source.getPort());
	        	 out.writeUTF(sb.toString());
	        	 in.readUTF();
	         } else if (type.equals("synctuple")) { 
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type + " " + target + " " + source.getHostname());
	        	 out.writeUTF(sb.toString());
	         } else if (type.equals("out") || type.equals("st")) {
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type + " "  + target + " " + datastr);
	        	 out.writeUTF(sb.toString());
	        	 if(type.equals("out")){
	        		 System.out.println("put " + target + " tuple " + originOutTupleText + " on " + serverName);
	        	 }
	        	 in.readUTF();
	         } else if (type.equals("in") || type.equals("rd") || type.equals("rm")) {
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type + " " + target + " " + hasVariable + " " + isSearch + " " + datastr);
	        	 out.writeUTF(sb.toString());
		         if(!hasVariable || !isSearch){
		        	 System.out.println(in.readUTF());
		         } else {
		        	 String inMsg = in.readUTF();
		        	 String[] inMsgSegment = inMsg.split(" ");
		        	 int hostID = Integer.parseInt(inMsgSegment[0]);
		        	 String serverType = inMsgSegment[1];
		        	 if(!P2.isResultFound() && hostID != -1){
//		        		 System.out.println("result found on " + serverType + " which host id is " + hostID);
		        		 P2.setResultPosition(hostID);
		        		 P2.setServerType(serverType);
		        		 P2.findResult();
		        	 }
		         }
	         } else if (type.equals("updatebkp")) {  
	        	 int size = backupMaps.size();
	        	 StringBuilder sb = new StringBuilder();
	        	 sb.append(type + " "  + size);
	        	 for (String key : backupMaps.keySet()) {
	        		 sb.append(" " + key + 
	        				   " " + backupMaps.get(key));
	        	 }
	        	 out.writeUTF(sb.toString());
	        	 in.readUTF();
	         } else if (type.equals("backup")) { 
//	        	 System.out.println("Client: request to calculate backup maps");
	        	 out.writeUTF("backup");
	         } else if (type.equals("recal")) { 
//	        	 System.out.println("Client: recalculate origin and backup tuple positions");
	        	 out.writeUTF(type);
	        	 in.readUTF();
	         } else if (type.equals("exit")) {
//	        	 System.out.println("Client: request to exit");
	        	 out.writeUTF("exit");
	         }
	         client.close();
		 } catch (IOException e) {
			 if(type.equals("update")){
				 System.out.println("target server cannot be reached.");
			 } else {
				 e.printStackTrace();
			 }
		 }		 
	 }
}