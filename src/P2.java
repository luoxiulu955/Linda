/**
 * P2 is the main class of the program. Process subcommand lines and manage the threads
 * 
 * @author Liang Xia
 */
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

public class P2 {
	static Map<String, String> hostMapping;
	static String customizedHost = "";
	static Server server;
	static boolean resultFound = false;
	static int resultPosition = -1;
	static String serverType = "";
	
	public static void main(String[] args) {
		
		String systemHost = "";
		String ipAddr = "";
		boolean isHostValid = false;
		Scanner scan = null;
		String text = "";
		
		if(args.length != 1) {
			System.out.println("Usage: java P1 $hostname/$ip ");
			System.out.println("It can be actual hostname or public ip address");
			System.out.println("Run hostname or ifconfig to get actual hostname and ip address");
			System.exit(0);
		} else {
			customizedHost = args[0];
		}
		
		PropertyReader properties = new PropertyReader();
		hostMapping = properties.getHosts();
		try {
			systemHost = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(1);
		}
		for(String key: hostMapping.keySet()) {
			if(systemHost.equals(key) || customizedHost.equals(hostMapping.get(key))) {
				isHostValid = true;
				ipAddr = hostMapping.get(key);
			}
		}
		
		if(!isHostValid) {
			System.out.println("IP address is not in the property file.");
			System.exit(0);
		}
		
		FileHandler fd = new FileHandler(customizedHost);
		
		server = new Server(customizedHost, ipAddr);
		server.start();
	    
		while(!text.equals("quit")) {
			System.out.print("linda> ");
			scan= new Scanner(System.in);
		    text = scan.nextLine();   
			subCommand(text, customizedHost, server);
//			System.out.println("print host info-----------------");
//			for(Host h : server.getHosts()){
//				System.out.println(h.toString());
//			}
//			System.out.println("--------------------------------");
//			System.out.println("print backup map -----------------");
//			for(String key : server.getBackupMaps().keySet()){
//				System.out.println("Original server: " + key + ", backup server: " + server.getBackupMaps().get(key));
//			}
//			System.out.println("--------------------------------");
//			System.out.println("virtual hosts number: " + server.getConsistentHashing().getVirtualNodesSize());
//			System.out.println("real hosts number: " + server.getConsistentHashing().getRealNodesSize());
//			System.out.println("origin tuple size: " + server.getTuples().size());
//			System.out.println("backup tuple size: " + server.getBackupTuples().size());
		}
		
		scan.close();
		System.exit(0);
	}
	
	private static void subCommand(String text, String customizedHost, Server server) {
		try {
			String serverIP = "";
			int serverPort = 0;
			String serverHost = "";
			
			if (text.startsWith("add")) {
				boolean isExist = false;
				boolean isValid = false;
				String str = text.replaceAll("\\s", "").replaceAll("[()]", " ");
				String[] hostlist = str.split("\\s+");
				
				if(hostlist.length <= 1 || !hostlist[0].equals("add")) {
					System.out.println("Invalid input of add command");
					System.out.println("Usage: add {($host, $ipaddr, $port)}");
					return;
				}
				
				for (String hostInfo : hostlist) {
					if(hostInfo.equals("add")){
						continue;
					}
					String[] words = hostInfo.split(",");
					if(words.length != 3) {
						System.out.println("Invalid input of add command");
						System.out.println("Usage: add {($host, $ipaddr, $port)}");
						return;
					}
					serverHost = words[0];
					serverIP = words[1];
					serverPort = Integer.valueOf(words[2]);
					
					for(Host h: server.getHosts()){
						if(h.getHostname().equals(serverHost)){
							isExist = true;
						}
					}
					
					for(String key: hostMapping.keySet()) {
						String[] syshostname = key.split("\\.");
//						System.out.println("syshostname size  " + syshostname.length);
						if(serverIP.equals(hostMapping.get(key)) || serverIP.equals(syshostname[0])){
							isValid = true;
							break;
						}
					}
					
					if(server.getMaster() == null){
						if(!isExist && isValid) {
							Client client = new Client(serverIP, serverPort, server.getHosts(), "add", customizedHost);
							client.start();
							client.join();
							Thread.sleep(100);
						} else if(isExist && isValid){
							System.out.println(serverHost + " is already used.");
							return;
						} else if (!isValid) {
							System.out.println(serverIP + " is not valid.");
							return;
						}
					} else {
						Host previousMaster = server.getMaster();
						if(hostAvailabilityCheck(server.getMaster().getIPAddr(), server.getMaster().getPort())){
							if(!isExist && isValid) {
								Client client = new Client(serverIP, serverPort, server.getHosts(), "add", customizedHost);
								client.start();
								client.join();
								Thread.sleep(100);
							} else if(isExist && isValid){
								System.out.println(serverHost + " is already added.");
								return;
							} else if (!isValid) {
								System.out.println(serverIP + " is not valid.");
								return;
							}
						} else {
							boolean isElected = true;
							for(Host h : server.getHosts()){
								h.setIsMaster(false);
								if(!h.getHostname().equals(server.getMaster().getHostname()) &&
								   !h.getHostname().equals(server.getHostname())) {
									if(isElected){
//										System.out.println("cureent server hostname: " + server.getHostname());
//										System.out.println("send elect message to " + h.getHostname());
										Client client = new Client(h.getIPAddr(), h.getPort(), "elect", server.getLocalHost(), server);
										client.start();
										client.join();
										isElected = server.getElected();
//										System.out.println("server.getElected(): " + server.getElected());
//										System.out.println("isElected: " + isElected);
//										System.out.println("give elect result: " + isElected +" from " + h.getHostname());
									}
								}
							}
							
							if(isElected){
								for(Host h : server.getHosts()) {	
									if(h.getHostname().equals(server.getHostname())) {
										h.setIsMaster(true);
										server.setMasterHost(h);
									} else {
										h.setIsMaster(false);
										if(!h.getHostname().equals(previousMaster.getHostname())){
//											System.out.println("send claim message to " + h.getHostname());
											Client client = new Client(h.getIPAddr(), h.getPort(), "claim", server.getLocalHost());
											client.start();
											client.join();
										}
									}
								}
							}
							
							boolean isMasterElected = false;
							while(!isMasterElected){
								for(Host h : server.getHosts()){
									if(h.getStatus()){
										isMasterElected = true;
									}
								}
							}
							
							if(!isExist && isValid) {
								Client client = new Client(serverIP, serverPort, server.getHosts(), "add", customizedHost);
								client.start();
								client.join();
								Thread.sleep(100);
							} else if(isExist && isValid){
								System.out.println(serverHost + " with " + serverIP + " is already added.");
								return;
							} else if (!isValid) {
								System.out.println(serverIP + " is not valid.");
								return;
							}
						}
					}
					
					//update current backupmap to new hosts
					Client client = new Client(serverIP, serverPort, "updatebkp", server.getBackupMaps());
					client.start();
					client.join();
				}
				isValid = false;
				isExist = false;
				
				//write backup maps after adding new nodes
				for (Host h : server.getHosts()) {
					Client client = new Client(h.getIPAddr(), h.getPort(), "backup");
					client.start();
					client.join();
				}
				
				//recalculate origin and backup tuple position
				for (Host h : server.getHosts()) {
					Client client = new Client(h.getIPAddr(), h.getPort(), "recal");
					client.start();
					client.join();
				}
				
			}
			
			if(text.startsWith("delete")) {
				text = text.replaceFirst("delete", "").replaceAll("\\s", "");
				if(!text.startsWith("(") || !text.endsWith(")")) {
					System.out.println("Usage: delte ($host {, $host})");
					System.out.println("hostname must be in the brackets");
					return;
				}
				text = text.replaceAll("[()]", "");
				String[] hosts = text.split(",");
				boolean isLocalDeleted = false;
				List<Host> deleteList = new ArrayList<>();
				List<Host> tmpList = new ArrayList<>(server.getHosts());
				for(String hostname : hosts) {
					boolean isExist = false;
					Host tmp = null;
					if(server.getHostname().equals(hostname)) {
						isLocalDeleted = true;
					}
					for(Host h : server.getHosts()) {
						if(h.getHostname().equals(hostname)) {
							isExist = true;
							tmp = h;
							deleteList.add(h);
						}
					}
					if (!isExist) {
						System.out.println(hostname + " is not existing.");
					} else {
//						List<Host> list = new ArrayList<>(server.getHosts());
						for (Host h : tmpList) {
							Client client = new Client(h.getIPAddr(), h.getPort(), "delete", tmp);
							client.start();
							client.join();
						}
					}
				}
				
				//calculate backup maps after deleting nodes
				//since some nodes lose the backup servers
				for (Host h : tmpList) {
					Client client = new Client(h.getIPAddr(), h.getPort(), "backup");
					client.start();
					client.join();
				}
//				System.out.println("print backup maps before recalculate tuples------");
//				for(String key : server.getBackupMaps().keySet()) {
//					System.out.println("origin server: " + key + ", backup server: " + server.getBackupMaps().get(key));
//				}
//				System.out.println("-------------------------------------------------");
				//recalculate origin and backup tuple position
				for (Host h : tmpList) {
//					System.out.println("request " + h.getHostname() + " to recalculate tuples");
					Client client = new Client(h.getIPAddr(), h.getPort(), "recal");
					client.start();
					client.join();
				}
				
				for(Host h : deleteList) {
					if(!h.getHostname().equals(server.getHostname())) {
						Client client = new Client(h.getIPAddr(), h.getPort(), "exit");
						client.start();
						client.join();
					}
				}
				if (isLocalDeleted) {
					FileHandler.cleanLocalFiles();
					System.out.println(server.getHostname() + " is deleted and program exits");
					System.exit(0);
				}
			}
			
			if(text.startsWith("out")){
				String origin = text = text.replaceFirst("out", "").replaceAll("\\s", "");
				if(!text.startsWith("(") || !text.endsWith(")")) {
					System.out.println("Usage: out($var1, $var2, $var3,.....)");
					System.out.println("value must be in the brackets");
					return;
				}
				
				text = text.replaceFirst("\\(", "");
				String str = text.substring(0, text.length() - 1);
				String[] values = str.split(",");
				Tuple tuple = new Tuple();
				
				if(values.length == 1 && values[0].isEmpty()){

				} else {
					for(String value : values) {
						if(isInteger(value)){
							Record r = new Record(Record.Type.INTEGER, value);
							tuple.addRecord(r);
						} else if (isFloat(value)){
							Record r = new Record(Record.Type.FLOAT, value);
							tuple.addRecord(r);
						} else if(value.startsWith("\"") && value.endsWith("\"")) {
							String v = value.substring(1, value.length() - 1);
							Record r = new Record(Record.Type.STRING, v);
							tuple.addRecord(r);
						} else {
							System.out.println("Value has wrong format.");
							tuple.clear();
							return;
						}
					}
				}
				
				List<Host> hosts = server.getHosts();
				String primaryServerName = server.getConsistentHashing().getServer(str);
				String backupServerName = server.getBackupMaps().get(primaryServerName);
				String backupServerIP = "";
	        	int backupServerPort = 0;
				for(Host h : hosts) {
	        		if(h.getHostname().equals(primaryServerName)){
	        			serverIP = h.getIPAddr();
	        			serverPort = h.getPort();
	        		}
	        		if(h.getHostname().equals(backupServerName)) {
	        			backupServerIP = h.getIPAddr();
	        			backupServerPort = h.getPort();
	        		}
	        	}
				if(hostAvailabilityCheck(serverIP, serverPort)) {
					Client c1 = new Client(serverIP, serverPort, "out", "origin", str, origin);
					c1.start();
				} else {
					System.out.println("original server is disconnected currently and cannot put tuple on it.");
				}
				if(hostAvailabilityCheck(backupServerIP, backupServerPort)) {
					Client c2 = new Client(backupServerIP, backupServerPort, "out", "backup", str, origin);
					c2.start();
				} else {
					System.out.println("backup server is disconnected currently and cannot put tuple on it.");
				}
			}
			
			if(text.startsWith("in") || text.startsWith("rd")){
				boolean hasVariable = false;
				List<Host> hosts = server.getHosts();
				String type;
				List<Client> clients;
				if(text.startsWith("in")) {
					type = "in";
				} else {
					type = "rd";
				}
				text = text.replaceFirst(type, "").replaceAll("\\s", "");
				if(!text.startsWith("(") || !text.endsWith(")")) {
					System.out.println("Usage: in($var1, $var2, $var3,.....)");
					System.out.println("value must be in the brackets");
					return;
				}
				
				text = text.replaceFirst("\\(", "");
				String str = text.substring(0, text.length() - 1);
				String[] values = str.split(",");
			
				for(String value : values) {
					if(value.startsWith("?")){
						String[] elem = value.split(":");
						if (elem.length != 2) {
							System.out.println("value includes wrong wild-card format");
							return;
						}
						String result = elem[0].replaceFirst("\\?", "");
						String varType = elem[1];
						if(result.equals("") || 
						   (!varType.equals("int") && !varType.equals("float") && !varType.equals("string"))) {
							System.out.println("value includes wrong wild-card format");
							return;
						}
						hasVariable = true;
					}
				}
				
				if(!hasVariable){
					String primaryServerName = server.getConsistentHashing().getServer(str);
					String backupServerName = server.getBackupMaps().get(primaryServerName);
					String backupServerIP = "";
		        	int backupServerPort = 0;
					for(Host h : hosts) {
		        		if(h.getHostname().equals(primaryServerName)){
		        			serverIP = h.getIPAddr();
		        			serverPort = h.getPort();
		        		}
		        		if(h.getHostname().equals(backupServerName)) {
		        			backupServerIP = h.getIPAddr();
		        			backupServerPort = h.getPort();
		        		}
		        	}
					if(hostAvailabilityCheck(serverIP, serverPort)) {
						Client client = new Client(serverIP, serverPort, type, "origin", str, hasVariable, false);
						client.start();
						client.join();
					} else {
						Client client = new Client(backupServerIP, backupServerPort, type, "backup", str, hasVariable, false);
						client.start();
						client.join();
					}
				} else {
					clients = new ArrayList<>();
					while(!isResultFound()){
						for(Host h : hosts){
				        	if(hostAvailabilityCheck(h.getIPAddr(), h.getPort())) {
								Client client = new Client(h.getIPAddr(), h.getPort(), type, "origin", str, hasVariable, true);
								clients.add(client);
								client.start();
				        	} else {
				        		String backupServer = server.getBackupMaps().get(h.getHostname());
				        		for(Host bkp_h : hosts) {
				        			if(bkp_h.getHostname().equals(backupServer)) {
				        				Client client = new Client(bkp_h.getIPAddr(), bkp_h.getPort(), type, "backup", str, hasVariable, true);
										clients.add(client);
										client.start();
										break;
				        			}
				        		}
				        	}
						}
						for(Client c : clients){
							c.join();
						}
					}
//					System.out.println("subcommand: found result on " + serverType + "host id: " + resultPosition);
					for(Host h : hosts){
						if(h.getID() == getResultPosition()){
							serverIP = h.getIPAddr();
							serverPort = h.getPort();
							break;
						}
					}
					Client client = new Client(serverIP, serverPort, type, serverType, str, hasVariable, false);
					client.start();
					client.join();
					
					resultFound = false;
					resultPosition = -1;
					serverType = "";
				}
			
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static boolean isInteger(String s) {
	    return isInteger(s,10);
	}

	public static boolean isInteger(String s, int radix) {
	    if(s.isEmpty()) return false;
	    for(int i = 0; i < s.length(); i++) {
	        if(i == 0 && s.charAt(i) == '-') {
	            if(s.length() == 1) return false;
	            else continue;
	        }
	        if(Character.digit(s.charAt(i),radix) < 0) return false;
	    }
	    return true;
	}
	
	public static boolean isFloat(String s) {
		 String decimalPattern = "\\-?([0-9]*)\\.([0-9]*)";  
		 boolean result = Pattern.matches(decimalPattern, s);
		 return result;
    }
	
	public static String getMD5String(String str){
		 MessageDigest md = null;
		 StringBuffer sb = new StringBuffer();
		 try {
			 md = MessageDigest.getInstance("MD5");
			 md.update(str.getBytes());
			 byte[] digest = md.digest();
			 for (byte b : digest) {
				 sb.append(String.format("%02x", b & 0xff));
			 }
		 } catch (NoSuchAlgorithmException e) {
			 e.printStackTrace();
		 }
		 return sb.toString();
	}
	
	public static boolean isResultFound(){
		return resultFound;
	}
	
	public static void findResult(){
		resultFound = true;
	}
	
	public static int getResultPosition(){
		return resultPosition;
	}

	public static void setResultPosition(int position){
		resultPosition = position;
	}
	
	public static String getServerType() {
		return serverType;
	}
	
	public static void setServerType(String type) {
		serverType = type;
	}
	
	public static boolean hostAvailabilityCheck(String ip, int port) { 
	    try (Socket s = new Socket(ip, port)) {
	        return true;
	    } catch (IOException ex) {
	        /* ignore */
	    }
	    return false;
	}
}
