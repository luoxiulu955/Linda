/**
 * Host object that store host information
 * @author Liang Xia
 *
 */
public class Host {
	private String hostname;
	private String ipAddr;
	private int port;
	private int id;
	private boolean isMaster;
	
	public Host(String hostname, String ipAddr, int port){
		id = 0;
		this.hostname = hostname;
		this.ipAddr = ipAddr;
		this.port = port;
		isMaster = false;
	}
	
	public Host(int id, String hostname, String ipAddr, int port, boolean isMaster){
		this.id = id;
		this.hostname = hostname;
		this.ipAddr = ipAddr;
		this.port = port;
		this.isMaster = isMaster;
	}
	
	public String getHostname(){
		return hostname;
	}
	
	public String getIPAddr(){
		return ipAddr;
	}
	
	public int getPort(){
		return port;
	}
	
	public int getID(){
		return id;
	} 
	
	public boolean getStatus() {
		return isMaster;
	}
	
	public void setID(int id){
		this.id = id;
	}
	
	public void setHostname(String hostname){
		this.hostname = hostname;
	}
	
	public void setIPAdder(String ipAddr) {
		this.ipAddr = ipAddr;
	}
	
	public void setPort(int port){
		this.port = port;
	}
	
	public void setIsMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}
	
	
	@Override
	public String toString(){
		return "host id: " + id + ", hostname: " + hostname + ", ip: " + ipAddr + ", port: " + port + ", isMaster" + isMaster;
	}
}
