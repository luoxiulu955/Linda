/**
 * Consistent Hashing class which is used to calculate the original and backup server
 * for the tuples.
 * It uses katama algorithm for calculation.
 * 
 * @author Liang Xia
 */
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashing{
	private List<String> realNodes;
	private final static int VIRTUAL_NODE_NUMBER = 10;
	private SortedMap<Integer, String> virtualNodes;
	
	public ConsistentHashing() {
		realNodes = new LinkedList<String>();
		virtualNodes = new TreeMap<Integer, String>();
	}
	
	public int getVirtualNodesSize() {
		return virtualNodes.size();
	}
	
	public int getRealNodesSize() {
		return realNodes.size();
	}
	
	public void addNode(Host host) {
		String realNodeHostname = host.getHostname();
		realNodes.add(realNodeHostname);
		for (int i = 0; i < VIRTUAL_NODE_NUMBER; i++) {
			String virtualNodeHostname = realNodeHostname + "&&VN" + String.valueOf(i);
			int slotNumber = getSlotNumber(virtualNodeHostname);
//			System.out.println("virtual node: " + virtualNodeHostname + " has slot: " + slotNumber);
			virtualNodes.put(slotNumber, virtualNodeHostname);
		}
		Collections.sort(realNodes);
	}
	
	public void deleteNode(Host host) {
		String realNodeHostname = host.getHostname();
		realNodes.remove(realNodeHostname);
		for (int i = 0; i < VIRTUAL_NODE_NUMBER; i++) {
			String virtualNodeHostname = realNodeHostname + "&&VN" + String.valueOf(i);
			int slotNumber = getSlotNumber(virtualNodeHostname);
//			System.out.println("remove virtual node: " + virtualNodeHostname);
			virtualNodes.remove(slotNumber);
		}
		Collections.sort(realNodes);
	}
	
	public String getServer(String tuple){
		int slotNumber = getSlotNumber(tuple);
		SortedMap<Integer, String> subMap = virtualNodes.tailMap(slotNumber);
		Integer i = subMap.firstKey();
		String realNode = subMap.get(i).split("&&")[0];
		return realNode;
	}
	
	//record the backup server only when put first tuple and backup server is deleted
	public String getBackupServer(String realNode) {
		int realNodeIndex = realNodes.indexOf(realNode);
		int realNodeNumber = realNodes.size();
		int backNodeIndex = (realNodeIndex + realNodeNumber / 2) % realNodeNumber;
		if(realNodeIndex == backNodeIndex && realNodeNumber > 1) {
			backNodeIndex++;
		}
		String backupNode = realNodes.get(backNodeIndex);
//		System.out.println("realnode index: " + realNodeIndex 
//				   		 + ", realnode number: " + realNodeNumber
//				   		 + ", backNode index: " + backNodeIndex
//				   		 + ", back node: " + backupNode);
		return backupNode;
	}
	
	private int getSlotNumber(String str) {
		int slotNumber = 0;
		String md5str = getMD5String(str);
		BigInteger md5BigInt = new BigInteger(md5str, 16);
		BigInteger slotSize = new BigInteger(Integer.toString((int)Math.pow(2,16)));
		slotNumber = md5BigInt.mod(slotSize).intValue();
		return slotNumber;
	}
	
	private String getMD5String(String str){
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
	
}
