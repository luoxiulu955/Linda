/**
 * Server object that stores all the information of the host and local tuples.
 * @author Liang Xia
 */
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;

public class Server extends Thread{
   private ServerSocket serverSocket;
   private String hostname;
   private int port;
   private String ipAddr;
   private Host localhost;
   private Host master;
   private List<Host> hosts;
   private List<Tuple> tuples;
   private List<Tuple> backuptuples;
   private Socket server;
   private boolean isElected;
   private int serialNumber;
   private ConsistentHashing ch;
//   private String backupServerName;
   private Map<String, String> backupMaps;
   private Lock readLock;
   private Lock writeLock;
   
   public Server(String host, String ipAddr) {
	  readLock = new ReentrantLock();
	  writeLock = new ReentrantLock();
	  backupMaps = new HashMap<>();
	  ch = new ConsistentHashing();
//	  backupServerName = null;
      this.hostname = host;
      serialNumber = 0;
      isElected = false;
      hosts = new ArrayList<>();
      tuples = new ArrayList<>();
      backuptuples = new ArrayList<>();
      this.ipAddr = ipAddr;
      initConnection();
   }
   
   private void initConnection() {
	  try {
		  InetAddress addr = InetAddress.getByName(ipAddr);
		  serverSocket = new ServerSocket(0, 10, addr);
	      port = serverSocket.getLocalPort();
	      master = null;
	      localhost = new Host(hostname, ipAddr, port);
	      hosts = FileHandler.loadHosts();
	      backupMaps = FileHandler.loadBackupMaps();
	      FileHandler.clearTuple();
	      FileHandler.writeHost(localhost, false);
	      if(hosts.size() == 0) {
	    	  hosts.add(localhost);
	    	  ch.addNode(localhost);
	    	  serialNumber++;
	      } else if (hosts.size() == 1) {
	    	  hosts.get(0).setPort(port);
	      } else {
	    	  recoverHostInfo();
	    	  try {
				  Thread.sleep(100);
			  } catch (InterruptedException e) {
				  e.printStackTrace();
			  }
	    	  recoverTuple();
	      }
		  System.out.println(ipAddr + " at port number: " + port);
	  } catch (IOException e) {
		  System.out.println(e.getMessage());
		  System.exit(1);
	  }
   }
   
   private void recoverHostInfo(){
	   String serverIP = "";
	   int serverPort = 0;
	   // update server's own port number
	   for (Host h : hosts){
 		  if (h.getHostname().equals(hostname)) {
 			  h.setPort(port);
 			  break;
 		  }
 	   }
	   
	   for (Host h : hosts){
 		  if (!h.getHostname().equals(hostname) &&
 			  P2.hostAvailabilityCheck(h.getIPAddr(), h.getPort())) {
 			  serverIP = h.getIPAddr();
 			  serverPort = h.getPort();
 			  break;
 		  }
 	   }
	   
	   try {
		   Client client = new Client(serverIP, serverPort, "synchosts", localhost, this);
		   client.start();
		   client.join();
	   } catch (InterruptedException e) {
		   e.printStackTrace();
	   }
   }
   
   private void recoverTuple(){
	   String originServerName = "";
	   String backupServerName = backupMaps.get(hostname);
	   Host backupServer = null;
	   List<Host> originServerList = new ArrayList<>();
	   for (String key : backupMaps.keySet()) {
		   if (backupMaps.get(key).equals(hostname)) {
			   originServerName = key;
			   for (Host h : hosts) {
				   if(h.getHostname().equals(originServerName)) {
					   originServerList.add(h);
				   }
			   }
		   }
	   }
	   for (Host h : hosts) {
		   if(h.getHostname().equals(backupServerName)) {
			   backupServer = h;
		   }
	   }

	   for (Host originServer : originServerList) {
		   requestSyncTuple(originServer, "origin");
	   }
	   
	   requestSyncTuple(backupServer, "backup");
	   
   }
   
   private void requestSyncTuple(Host h, String serverType) {
	   try {
		   Client client = new Client(h.getIPAddr(), h.getPort(), "synctuple", serverType, localhost);
		   client.start();
		   client.join();
	   } catch (InterruptedException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
	   }
   }
   
   public Lock getWriteLock(){
	   return writeLock;
   }
   
   public Lock getReadLock() {
	   return readLock;
   }
   
   public Map<String, String> getBackupMaps(){
	   return backupMaps;
   }
   
   public ConsistentHashing getConsistentHashing() {
	   return ch;
   }
   
   public int getSerialNumber() {
	   return serialNumber;
   }
   
   public void setSerialNumber(int serialNumber) {
	   this.serialNumber = serialNumber;
   }
   
   public void setElected(boolean result){
	   isElected = result;
   }
   
   public boolean getElected(){
	   return isElected;
   }
   
   public Socket getSocket(){
	   return server;
   }
   
   public String getHostname(){
	   return hostname;
   }
   
   public String getIP(){
	   return ipAddr;
   }
   
   public Host getLocalHost(){
	   return localhost;
   }
   
   public Host getMaster(){
	   return master;
   }
   
   public List<Tuple> getTuples(){
	   return tuples;
   }
   
   public List<Tuple> getBackupTuples(){
	   return backuptuples;
   }
   
   public int getPort(){
	   return port;
   }
   
   public List<Host> getHosts(){
	   return hosts;
   }
   
   public void setMasterHost(Host h){
	   this.master = h;
   }
   
   public void run(){
	   while(true) {
		  try {
	            server = serverSocket.accept();
		  }catch(IOException e) {
	             e.printStackTrace();
	      }
		  new EchoThread(this).start();
	   }
   }

}