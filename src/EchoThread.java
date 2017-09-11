/**
 * Server thread that accept multiple clients' requests simultaneously and process requests
 * @author Liang Xia
 */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class EchoThread extends Thread{
		private Socket serverSocket;
		private String hostname;
		private Host localhost;
		private Host master;
		private List<Host> hosts;
		private List<Tuple> tuples;
		private List<Tuple> backuptuples;
		private ConsistentHashing ch;
		private Server server;
		private Lock readLock;
		private Lock writeLock;
		
		public EchoThread(Server server){
			this.writeLock = server.getWriteLock();
			this.readLock = server.getReadLock();
			this.server = server;
			this.serverSocket = server.getSocket();
			this.hostname = server.getHostname();
			this.localhost = server.getLocalHost();
			this.master = server.getMaster();
			this.hosts = server.getHosts();
			this.tuples = server.getTuples();
			this.backuptuples = server.getBackupTuples();
			this.ch = server.getConsistentHashing();
		} 
		
		public void run() {
			String[] str = null;
			try {
		         DataInputStream in = new DataInputStream(serverSocket.getInputStream());
		         OutputStream backToClient = serverSocket.getOutputStream();
			     DataOutputStream out = new DataOutputStream(backToClient);
		         String receiveData = in.readUTF();
		         str = receiveData.split(" ");
		         if(str[0].equals("add")){
		         	 int count = Integer.valueOf(str[1]);
		         	 for(int i = 0; i < count; i++){
		            	 int id = Integer.valueOf(str[2+i*5]);
		             	 String hostname = str[3+i*5];
		            	 String ipAddr = str[4+i*5];
		            	 int port = Integer.valueOf(str[5+i*5]);
		            	 boolean isMaster = Boolean.parseBoolean(str[6+i*5]);
		            	 Host tmp = new Host(id, hostname, ipAddr, port, isMaster);
		            	 if(isMaster) {
		            		 master = new Host(id, hostname, ipAddr, port, isMaster);
		            		 server.setMasterHost(master);
		            	 }
		            	 System.out.println("add host " + tmp.getHostname() + ", ip " + tmp.getIPAddr() + ", port " +tmp.getPort());
		            	 hosts.add(tmp);
		            	 ch.addNode(tmp);
		            	 server.setSerialNumber(server.getSerialNumber() + 1);
		            	 FileHandler.writeHost(tmp, true);
		         	 }
		         	 requestID("getID", master);
		         } else if (str[0].equals("delete")) { 
		        	 String remoteHostname = str[1];
//		        	 System.out.println("Server: delete host " + remoteHostname + "from the hostlist");
		        	 Host tmp = null;
		        	 for (Host h : hosts) {
	        			 if (h.getHostname().equals(remoteHostname)) {
	        				 tmp = h;
	        			 }
	        		 }
		        	 System.out.println("delete " + tmp.getHostname());
//		        	 System.out.println("delete " + tmp.getHostname() + "from the hostlist and nets file");
	        		 hosts.remove(tmp);
	        		 ch.deleteNode(tmp);
	        		 FileHandler.deleteHost(tmp);
	        		 //clear its original server's backup
//	        		 System.out.println("clear its original server's backup----");
	        		 for(String key : server.getBackupMaps().keySet()) {
//	        			 System.out.println("get original server " + key);
	        			 if(server.getBackupMaps().get(key) != null && 
	        			    server.getBackupMaps().get(key).equals(remoteHostname)) {
//	        				 System.out.println("set origianl server " + key + "'s backup server " + remoteHostname + "to null");
	        				 server.getBackupMaps().put(key, null);
	        			 }
	        		 }
//	        		 System.out.println("--------------------------------------");
	        		 //delete server's backup map record
	        		 server.getBackupMaps().remove(remoteHostname);
		        	 out.writeUTF("true");
		         } else if (str[0].equals("elect")) {
//		        	 String remoteHostname = str[1];
		        	 String remoteIP = str[2];
		        	 int remotePort = Integer.parseInt(str[3]);
		        	 boolean isElected = true;
		        	 Host previousMaster = master;
//		        	 System.out.println("Server: get elect request from " + remoteHostname);
		        	 if(server.getIP().compareTo(remoteIP) > 0) {
		        		 isElected = true;
		        	 } else if (server.getIP().compareTo(remoteIP) == 0) {
		        		 if(server.getPort() > remotePort) {
		        			 isElected = true;
		        		 } else {
		        			 isElected = false;
		        		 }
		        	 } else {
		        		 isElected = false;
		        	 }
		        	 
		        	 if(!isElected){
		        		 out.writeUTF("true");
//		        		 System.out.println("Server: write true back to client");
//		        		 System.out.println("Server: lose the election from " + remoteHostname);
		        	 } else {
		        		 out.writeUTF("false");
//		        		 System.out.println("Server: win the election from " + remoteHostname);
//		        		 System.out.println("old master hostname is " + server.getMaster().getHostname());
		        		 for(Host h : hosts){
							if(!h.getHostname().equals(server.getMaster().getHostname()) &&
							   !h.getHostname().equals(server.getHostname())) {
								if(isElected){
									try {
										Client client = new Client(h.getIPAddr(), h.getPort(), "elect", server.getLocalHost(), server);
										client.start();
										client.join();
										isElected = server.getElected();
									} catch (InterruptedException e) {
										System.out.println(h.getHostname() + " is not available currently");
									}
								}
							}
						}
		        	}
		        	if(isElected){
//		        		System.out.println("Server: win the election and send claim message to others");
						for(Host h : hosts) {	
							if(h.getHostname().equals(server.getHostname())) {
								h.setIsMaster(true);
								server.setMasterHost(h);
							} else {
								h.setIsMaster(false);
								if(!h.getHostname().equals(previousMaster.getHostname())){
									try {
										Client client = new Client(h.getIPAddr(), h.getPort(), "claim", server.getLocalHost());
										client.start();
										client.join();
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}
							}
						}
					} else {
//						System.out.println("Server: Wait for new elected master.");
					}
		         } else if(str[0].equals("claim")) {
		        	 String masterHostname = str[1];
		        	 for(Host h : hosts){
		        		 if(h.getHostname().equals(masterHostname)) {
		        			 h.setIsMaster(true);
		        			 server.setMasterHost(h);
						 } else {
							 h.setIsMaster(false);
						 }
		        	 }
		         } else if(str[0].equals("getID")) {
		         	int id = server.getSerialNumber();
		         	String hostname = str[1];
		         	String ipAddr = str[2];
		         	int port = Integer.valueOf(str[3]);
		         	boolean isMaster = Boolean.parseBoolean(str[4]);
		         	Host tmp = new Host(id, hostname, ipAddr, port, isMaster);
			        out.writeUTF(String.valueOf(id));
			        for(Host host : hosts){
			        	if(!host.getHostname().equals(this.hostname)){
			        		syncHosts("update", host, tmp);
			        	}
			        }
		         	hosts.add(tmp);
		         	System.out.println("add host " + tmp.getHostname() + ", ip " + tmp.getIPAddr() + ", port " +tmp.getPort());
		         	ch.addNode(tmp);
		         	server.setSerialNumber(server.getSerialNumber() + 1);
		         	FileHandler.writeHost(tmp, true);
		         } else if(str[0].equals("update")){
					int id = Integer.valueOf(str[1]);
					String hostname = str[2];
					String ipAddr = str[3];
					int port = Integer.valueOf(str[4]);
					boolean isMaster = Boolean.parseBoolean(str[5]);
					Host tmp = new Host(id, hostname, ipAddr, port, isMaster);
					hosts.add(tmp);
					System.out.println("add host " + tmp.getHostname() + ", ip " + tmp.getIPAddr() + ", port " +tmp.getPort());
					ch.addNode(tmp);
					server.setSerialNumber(server.getSerialNumber() + 1);
					FileHandler.writeHost(tmp, true);
		         } else if (str[0].equals("synchosts")){
		        	 String remoteHostname = str[1];
		        	 int remotePort = Integer.parseInt(str[2]);
//		        	 System.out.println("Server: sync hosts from " + remoteHostname);
		        	 StringBuilder sb = new StringBuilder();
		        	 int count = hosts.size();
		        	 sb.append(count);
		        	 boolean isRemoteExist = false;
		        	 for (Host host : hosts) {
		        		 //update recovered host's port number
		        		 if(host.getHostname().equals(remoteHostname)) {
		        			 isRemoteExist = true;
		        			 host.setPort(remotePort);
		        			 FileHandler.updateHost(host);
		        		 }
		        		 sb.append(" " + host.getID() + 
		        				   " " + host.getHostname() + 
		        				   " " + host.getIPAddr() + 
		        				   " " + host.getPort() +
		        				   " " + host.getStatus());
		        	 }
		        	 if(isRemoteExist) {
//		        		 System.out.println("Server: send hosts to " + remoteHostname);
			        	 out.writeUTF(sb.toString());
			        	 syncUpdate(remoteHostname, remotePort);
		        	 } else {
		        		 out.writeUTF("refuse");
		        	 }
		         } else if (str[0].equals("syncupdate")){
		        	 String remoteHostname = str[1];
		        	 int remotePort = Integer.parseInt(str[2]);
		        	 for(Host host: hosts) {
		        		 if(host.getHostname().equals(remoteHostname)) {
		        			 host.setPort(remotePort);
		        		 }
		        		 FileHandler.updateHost(host);
		        	 }
		        	 out.writeUTF("done");
		         } else if (str[0].equals("synctuple")){
		        	 String serverType = str[1];
		        	 String destHostname = str[2];
		        	 Host destHost = null;
		        	 for (Host h : hosts) {
		        		 if(h.getHostname().equals(destHostname)) {
		        			 destHost = h;
		        			 break;
		        		 }
		        	 }
		        	 sendTuple(destHost, serverType);
		         } else if (str[0].equals("out") || str[0].equals("st")){
		        	writeLock.lock();
//		        	System.out.println("Server: get out request: " + receiveData);
		        	Tuple tuple;
		        	String target = str[1];
		        	String result = target;
		        	if(str.length == 2){
		        		tuple = new Tuple();
		        	} else {
		        		result = target + " " + str[2];
			         	String[] datastr = str[2].split(",");
			         	tuple = getTuple(datastr);
		        	}
		        	if(target.equals("origin")) {
		        		tuples.add(tuple);
		        	} else {
		        		backuptuples.add(tuple);
		        	}
		        	System.out.println("store " + target + " tuple (" + tuple.toString()+ ") in the " + target + " list");
					FileHandler.writeTuple(result);
					out.writeUTF("done");
					writeLock.unlock();
		         } else if(str[0].equals("in") || str[0].equals("rd") || str[0].equals("rm")){
		        	readLock.lock();
		        	String serverType = str[1];
		        	boolean hasVariable = Boolean.parseBoolean(str[2]);
		        	boolean isSearch = Boolean.parseBoolean(str[3]);
		        	Tuple target;
//		        	System.out.println("Server: get message request: " + receiveData);
//		        	System.out.println("serverType: " + serverType + ", hasvariable: " + hasVariable + ", isSearch: " + isSearch);
//		        	System.out.println("Server: str length is " + str.length);
		        	if(str.length == 4){
		        		target = new Tuple();
		        	} else {
		        		String[] datastr = str[4].split(",");
			         	target = getTuple(datastr);
		        	}
		         	List<Record> target_list = target.getRecords();
		         	int target_length = target_list.size();
		         	boolean isFound = false;
		         	Tuple tmp = null;
//		         	List<Tuple> tupleList = getTupleList(serverType);
		         	
//		         	System.out.println(" Tuple list size is " + tupleList.size());
//		         	System.out.println("Print tuplelist---------------------");
//		         	for (Tuple tuple : tupleList) {
//		         		System.out.println(tuple.toString());
//		         	}
//		         	System.out.println("---------------------");
		         	if(!isSearch){
			         	while(!isFound){
//			         		List<Tuple> tupleList = getTupleList(serverType); 
			         		for(Tuple source : getTupleList(serverType)) {
			     			   List<Record> source_list = source.getRecords();
			     			   int source_length = source_list.size();
			            		   if(source_length == target_length) {
			            			   boolean isMatched = true;
			            			   if(target_length != 0){
			            				   for(int j = 0; j < target_length; j++) {
			            					   String target_value = target_list.get(j).getValue();
			            					   String source_value = source_list.get(j).getValue();
			            					   Record.Type target_type = target_list.get(j).getType();
			            					   Record.Type source_type = source_list.get(j).getType();
			            					   if(target_type != source_type) {
			            						   isMatched = false;
			            						   break;
			            					   } else {
			            						   if (target_value != null && !target_value.equals(source_value)){
			            							   isMatched = false;
			            							   break;
			            						   }
			            					   }
			            				   }
			            			   }
			            			   if(isMatched){
			            				   isFound = true;
			            				   tmp = source;
//			            				   System.out.println("result found: " + tmp.toString());
			            				   break;
			            			   }
			            		  }
			            	}
				        }
		         	} else {
//		         		tupleList = getTupleList(serverType);
		         		for(Tuple source : getTupleList(serverType)) {
		     			   List<Record> source_list = source.getRecords();
		     			   int source_length = source_list.size();
		            		   if(source_length == target_length) {
		            			   boolean isMatched = true;
		            			   if(target_length != 0){
		            				   for(int i = 0; i < target_length; i++) {
		            					   String target_value = target_list.get(i).getValue();
		            					   String source_value = source_list.get(i).getValue();
		            					   Record.Type target_type = target_list.get(i).getType();
		            					   Record.Type source_type = source_list.get(i).getType();
		            					   if(target_type != source_type) {
		            						   isMatched = false;
		            						   break;
		            					   } else {
		            						   if (target_value != null && !target_value.equals(source_value)){
		            							   isMatched = false;
		            							   break;
		            						   }
		            					   }
		            				   }
		            			   }
		            			   if(isMatched){
		            				   isFound = true;
		            				   tmp = source;
		            				   break;
		            			   }
		            		  }
		            	}
		         	}
		         	String result = "";
		         	if(isFound){
			         	StringBuilder sb = new StringBuilder();
			         	for(Record r : tmp.getRecords()){
			         		if(sb.length() != 0){
			         			sb.append(",");
			         		}
			         		if(r.getType() == Record.Type.STRING){
			         			sb.append("\"" + r.getValue() + "\"");
			         		} else {
			         			sb.append(r.getValue());
			         		}
			         	}
			         	result = sb.toString();
		         	}
		         	
		         	if (!hasVariable){
//		         		System.out.println("Server: send tuple to remote: " + serverSocket.getInetAddress() + ":" + serverSocket.getPort());
				        if(str[0].equals("rm")) {
				        	System.out.println("backup tuple (" + result + ") is deleted");
				        	out.writeUTF("delete backup tuple (" + result + ") on " + localhost.getIPAddr());
				        	if (serverType.equals("origin")) {
				        		tuples.remove(tmp);
				        	} else {
				        		backuptuples.remove(tmp);
				        	}
				        	if(result.equals("")) {
				        		FileHandler.deleteTuple(serverType);
				        	} else {
				        		FileHandler.deleteTuple(serverType + " " + result);
				        	}
				        } else {
				        	System.out.println("find tuple (" + result + ")");
			         		out.writeUTF("get tuple (" + result + ") on " + serverType + ": " + localhost.getIPAddr());
				         	if(str[0].equals("in")){
				         		if (serverType.equals("origin")) {
					        		tuples.remove(tmp);
					        	} else {
					        		backuptuples.remove(tmp);
					        	}
				         		if(result.equals("")) {
					        		FileHandler.deleteTuple(serverType);
					        	} else {
					        		FileHandler.deleteTuple(serverType + " " + result);
					        	}
				         		if(serverType.equals("origin")){
				         			removeTupleOnBackupHost(result);
				         		}
				         	}
				        }
		         	} else {
		         		if(isSearch){
		         			if(isFound){
		         				int myid = 0;
		         				for(Host h : hosts) {
		         					if(h.getHostname().equals(server.getHostname())) {
		         						myid = h.getID();
		         					}
		         				}
		         				out.writeUTF(String.valueOf(myid + " " + serverType));
//		         				System.out.println("result found on " + serverType + "which host id is " + myid);
		         			} else {
		         				out.writeUTF("-1 " + serverType);
//		         				System.out.println("result not found on " + serverType);
		         			}
		         		} else {
//		         			System.out.println("Server: send tuple to remote: " + serverSocket.getInetAddress() + ":" + serverSocket.getPort());
		         			System.out.println("find tuple (" + result + ")");
		         			out.writeUTF("get tuple (" + result + ") on " + serverType + ": " + localhost.getIPAddr());
				         	if(str[0].equals("in")){
				         		if (serverType.equals("origin")) {
					        		tuples.remove(tmp);
					        	} else {
					        		backuptuples.remove(tmp);
					        	}
				         		if(result.equals("")) {
					        		FileHandler.deleteTuple(serverType);
					        	} else {
					        		FileHandler.deleteTuple(serverType + " " + result);
					        	}
				         		if(serverType.equals("origin")){
				         			removeTupleOnBackupHost(result);
				         		}
				         	}
		         		}
		         	}
		         	readLock.unlock();
		         } else if(str[0].equals("updatebkp")) { 
		        	 int size = Integer.parseInt(str[1]);
		        	 Map<String, String> backupMaps = server.getBackupMaps();
		        	 for (int i = 0; i < size; i++) {
		        		 backupMaps.put(str[i * 2 + 2], str[i * 2 + 3]);
		        	 }
		        	 out.writeUTF("done");
		         } else if(str[0].equals("backup")) { 
		        	 for(Host h : server.getHosts()) {
						 if(!server.getBackupMaps().containsKey(h.getHostname())) {
						 	 String backupServer = server.getConsistentHashing().getBackupServer(h.getHostname());
							 server.getBackupMaps().put(h.getHostname(), backupServer);
						 } else {
							 if (server.getBackupMaps().get(h.getHostname()) == null) {
								 String backupServer = server.getConsistentHashing().getBackupServer(h.getHostname());
								 server.getBackupMaps().put(h.getHostname(), backupServer);
							 }
						 }
					 }
		        	 FileHandler.writeBackupMap(server.getBackupMaps());
		        	 out.writeUTF("done");
		         } else if(str[0].equals("recal")) { 
		        	 //move origin and backup tuples
		        	 //delete them in list and files
		        	 moveOriginalTuples();
		        	 moveBackupTuples();
		        	 out.writeUTF("done");
		         } else if(str[0].equals("exit")) {
		        	 FileHandler.cleanLocalFiles();
		        	 System.out.println(server.getHostname() + "is deleted and program exits");
		        	 System.exit(0);
		         }
		         serverSocket.close(); 
			} catch (IOException e) {
				 //e.printStackTrace();
			}		
		}
	
		private void removeTupleOnBackupHost(String datastr) {
			Map<String, String> backupMaps = server.getBackupMaps();
			String backupHost = backupMaps.get(server.getHostname());
			String serverType = "backup";
			Host targetHost = null;
     		for (Host h : hosts) {
     			if(h.getHostname().equals(backupHost)) {
     				targetHost = h;
     				break;
     			}
     		}
     		if(P2.hostAvailabilityCheck(targetHost.getIPAddr(), targetHost.getPort())) {
//     			System.out.println("delete tuple on backup server: " + backupHost + " as well.");
     			try {
	     			Client client = new Client(targetHost.getIPAddr(), targetHost.getPort(), "rm", serverType, datastr, false, false);
					client.start();
					client.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
     		} else {
     			System.out.println("cannot delete tuple on backup server: " + backupHost + " since it's disconnected");
     		}
			
		}

		private void moveBackupTuples() {
			 List<Tuple> tmplist = new ArrayList<>(server.getBackupTuples());
			 for (Tuple tuple : tmplist) {
	       		 String result = getTupleString(tuple);
	       		 String origin = "(" + result + ")";
	       		 String originServerName = server.getConsistentHashing().getServer(result);
	       		 String targetServerName = server.getBackupMaps().get(originServerName);
//	       		 System.out.println("new backup server is " + targetServerName);
	       		 if(!targetServerName.equals(server.getHostname())) {
	       			 server.getBackupTuples().remove(tuple);
	       			 if (result.equals("")) {
	       				 FileHandler.deleteTuple("backup");
	       			 } else {
	       				 FileHandler.deleteTuple("backup" + " " + result);
	       			 }
	       			 Host tmp = null;
	       			 for (Host h : hosts) {
	       				 if(h.getHostname().equals(targetServerName)) {
	       					 tmp = h;
	       					 break;
	       				 }
	       			 }
	     	         try {
	     	        	 System.out.println("send backup tuple " + origin + " to " + tmp.getHostname() + " " + tmp.getIPAddr() + " " + tmp.getPort());
	     		         Client client = new Client(tmp.getIPAddr(), tmp.getPort(), "st", "backup", result, origin);
     		             client.start();
     				     client.join();
	     			 } catch (InterruptedException e) {
     				     e.printStackTrace();
	     			 }
	       		  }
			 }
		}

		private void moveOriginalTuples() {
			 List<Tuple> tmplist = new ArrayList<>(server.getTuples());
				 for (Tuple tuple : tmplist) {
	       		 String result = getTupleString(tuple);
	       		 String origin = "(" + result + ")";
	       		 String targetServerName = server.getConsistentHashing().getServer(result);
//	       		 System.out.println("new origin server is " + targetServerName);
	       		 if(!targetServerName.equals(server.getHostname())) {
	       			 server.getTuples().remove(tuple);
	      			 if (result.equals("")) {
	      				 FileHandler.deleteTuple("origin");
	      			 } else {
	      				 FileHandler.deleteTuple("origin" + " " + result);
	      			 }
	       			 Host tmp = null;
	       			 for (Host h : hosts) {
	       				 if(h.getHostname().equals(targetServerName)) {
	       					 tmp = h;
	       					 break;
	       				 }
	       			 }
	     	         try {
	     	        	 System.out.println("send origin tuple " + origin + " to " + tmp.getHostname() + " " + tmp.getIPAddr() + " " + tmp.getPort());
     		             Client client = new Client(tmp.getIPAddr(), tmp.getPort(), "st", "origin", result, origin);
     		             client.start();
     				     client.join();
	     			  } catch (InterruptedException e) {
     				     e.printStackTrace();
	     			  }
	       		 }
			 }
		}

		private Tuple getTuple(String[] datastr){
		   Tuple tuple = new Tuple();
			for(String value : datastr) {
				if(P2.isInteger(value)){
					Record r = new Record(Record.Type.INTEGER, value);
					tuple.addRecord(r);
				} else if (P2.isFloat(value)){
					Record r = new Record(Record.Type.FLOAT, value);
					tuple.addRecord(r);
				} else if(value.startsWith("\"") && value.endsWith("\"")) {
					String v = value.substring(1, value.length() - 1);
					Record r = new Record(Record.Type.STRING, v);
					tuple.addRecord(r);
				} else if(value.startsWith("?")){
					String[] s = value.split(":");
					String type = s[1];
					Record r = null;
					if(type.toLowerCase().equals("int")) {
						r = new Record(Record.Type.INTEGER, null);
					}
					if (type.toLowerCase().equals("string")){
						r = new Record(Record.Type.STRING, null);
					}
					if (type.toLowerCase().equals("float")){
						r = new Record(Record.Type.FLOAT, null);
					}
					tuple.addRecord(r);
				}
			}
			return tuple;
	   }
		
	   private void sendTuple(Host h, String serverType) {
		   List<Tuple> tupleList = getTupleList(serverType);
		   String oppositeServerType = "";
		   if (serverType.equals("origin")) {
			   oppositeServerType = "backup";
		   } else {
			   oppositeServerType = "origin";
		   }
		   for (Tuple tuple : tupleList) {
			   String result = getTupleString(tuple);
	           String origin = "(" + result + ")";
	           try {
//	        	   System.out.println("send tuple " + origin + " to " + h.getIPAddr() + " " + h.getPort());
		           Client client = new Client(h.getIPAddr(), h.getPort(), "st", oppositeServerType, result, origin);
		           client.start();
				   client.join();
			   } catch (InterruptedException e) {
				   e.printStackTrace();
			   }
		   }
	   }
	   
	   private String getTupleString(Tuple tuple) {
		   StringBuilder sb = new StringBuilder();
           for(Record r : tuple.getRecords()){
         	  if(sb.length() != 0){
         	   	  sb.append(",");
         	  }
         	  if(r.getType() == Record.Type.STRING){
         		  sb.append("\"" + r.getValue() + "\"");
         	  } else {
         		  sb.append(r.getValue());
         	  }
           }
           return sb.toString();
	   }
	   
	   private void requestID(String type, Host target) {
			try {
				Client client = new Client(target.getIPAddr(), target.getPort(), hosts, type, hostname);
				client.start();
				client.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	   }
	   
	   private void syncHosts(String type, Host target, Host source){
			try {
				Client client = new Client(target.getIPAddr(), target.getPort(), type, source);
				client.start();
				client.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	   }
	   
	   private void syncUpdate(String targetHostname, int targetPort) {
		   Host source = new Host(targetHostname, null, targetPort);
		   for (Host h : hosts) {
			   if(!h.getHostname().equals(targetHostname) && !h.getHostname().equals(hostname)) {
				   try {
//					    System.out.println("Client: update " + targetHostname + " to " + h.getHostname());
						Client client = new Client(h.getIPAddr(), h.getPort(), "syncupdate", source);
						client.start();
						client.join();
				   } catch (InterruptedException e) {
						e.printStackTrace();
				   }
			   }
		   }
	   }
	   
	   private List<Tuple> getTupleList(String serverType){
		   if(serverType.equals("origin")) {
			   return tuples;
		   }
		   return backuptuples;
	   }
}
