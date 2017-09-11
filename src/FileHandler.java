/**
 * File Reader and Writer check if folders and files are existing.
 * If not, create them with correct permission.
 * Read and write values of tuples and hosts in and from the file. 
 * 
 * @author Liang Xia
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FileHandler {

	private static String NETSFILENAME = "nets";
	private static String TUPLESFILENAME = "tuples";
	private static String BACKUPMAPFILENAME = "backupmaps";
	private static String LOGINFOLDERNAME = "lxia";
	private static String LINDAFOLDERNAME = "linda";
	private static String customizedHost;
	private static String loginFolderPath;
	private static String lindaFolderPath;
	private static String nameFolderPath;
	private static String netsFilePath;
	private static String tuplesFilePath;
	private static String backupmapsFilePath;
	private static File login;
	private static File linda;
	private static File name;
	private static File nets;
	private static File tuples;
	private static File backupmaps;
	private static Lock lock;
	
	public FileHandler(String customizedHost){
		lock = new ReentrantLock();
		FileHandler.customizedHost = customizedHost;
		initFolderAndFile();
	}
	
	private void initFolderAndFile(){
		loginFolderPath = "/tmp/" + LOGINFOLDERNAME;
		lindaFolderPath = loginFolderPath + "/" + LINDAFOLDERNAME;
		nameFolderPath = lindaFolderPath + "/" + customizedHost;
		netsFilePath = nameFolderPath + "/" + NETSFILENAME;
		tuplesFilePath = nameFolderPath + "/" + TUPLESFILENAME;
		backupmapsFilePath = nameFolderPath + "/" + BACKUPMAPFILENAME;
		
		login = new File(loginFolderPath);
		linda = new File(lindaFolderPath);
		name = new File(nameFolderPath);
		nets = new File(netsFilePath);
		tuples = new File(tuplesFilePath);
		backupmaps = new File(backupmapsFilePath);
		
		//using PosixFilePermission to set folder permissions 777
        Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
        //add owners permission
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        //add group permissions
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);
        perms.add(PosixFilePermission.GROUP_EXECUTE);
        //add others permissions
        perms.add(PosixFilePermission.OTHERS_READ);
        perms.add(PosixFilePermission.OTHERS_WRITE);
        perms.add(PosixFilePermission.OTHERS_EXECUTE);
		
        try {
			if(!login.exists() || !login.isDirectory()) {
				login.mkdir();
				Files.setPosixFilePermissions(Paths.get(loginFolderPath), perms);
			}
			if(!linda.exists() || !linda.isDirectory()) {
				linda.mkdir();
				Files.setPosixFilePermissions(Paths.get(lindaFolderPath), perms);
			}
			if(!name.exists() || !name.isDirectory()) {
				name.mkdir();
				Files.setPosixFilePermissions(Paths.get(nameFolderPath), perms);
			}
			
			//remove execute permissions for files
			perms.remove(PosixFilePermission.OWNER_EXECUTE);
			perms.remove(PosixFilePermission.GROUP_EXECUTE);
			perms.remove(PosixFilePermission.OTHERS_EXECUTE);
			if(!nets.exists() || !nets.isFile()) {
				nets.createNewFile();
				Files.setPosixFilePermissions(Paths.get(netsFilePath), perms);
			}
			if(!tuples.exists() || !tuples.isFile()) {
				tuples.createNewFile();
				Files.setPosixFilePermissions(Paths.get(tuplesFilePath), perms);
			}
			if(!backupmaps.exists() || !backupmaps.isFile()) {
				backupmaps.createNewFile();
				Files.setPosixFilePermissions(Paths.get(backupmapsFilePath), perms);
			}
        } catch (IOException e) {
			e.printStackTrace();
		}    
	}
	
	public static List<Tuple> loadTuples(String serverType){
		List<Tuple> tuplelist = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(tuples));
			String line;
			while ((line = br.readLine()) != null) {
				String[] str = line.split(" ");
				if(str[0].equals(serverType)){
					String[] tupleString = str[1].split(",");
					Tuple tmp = getTuple(tupleString);
					tuplelist.add(tmp);
				}
			}
            br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tuplelist;
	}
	
	public static List<Host> loadHosts(){
		List<Host> hosts = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(nets));
			String line;
			while ((line = br.readLine()) != null) {
				String[] str = line.split(" ");
				if(str.length == 3){
					String hostname = str[0];
					String ip = str[1];
					int port = Integer.parseInt(str[2]);
					Host tmp = new Host(hostname, ip, port);
					hosts.add(tmp);
				}
			}
            br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return hosts;
	}
	
	public static Map<String, String> loadBackupMaps(){
		Map<String, String> backupMaps = new HashMap<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(backupmaps));
			String line;
			while ((line = br.readLine()) != null) {
				String[] str = line.split(" ");
				if(str.length == 2){
					String origin = str[0];
					String backup = str[1];
					backupMaps.put(origin, backup);
				}
			}
            br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return backupMaps;
	}
	
	public static void writeBackupMap(Map<String, String> backupMaps) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(backupmaps));
			for (String origin : backupMaps.keySet()) {
				bw.write(origin + " " + backupMaps.get(origin));
				bw.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally { 
			   try{
			      if(bw!=null)
			    	  bw.close();
			   }catch(Exception ex){
			       System.out.println("Error in closing the BufferedWriter"+ex);
			   }
		}
	}
	
	public static void writeTuple(String datastr) {
		lock.lock();
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(tuples, true));
			bw.write(datastr);
			bw.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}finally { 
			   try{
				  lock.unlock();
			      if(bw!=null)
			    	  bw.close();
			   }catch(Exception ex){
			       System.out.println("Error in closing the BufferedWriter"+ex);
			   }
		}
	}
	
	public static void writeHost(Host host, boolean isAppend){
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(nets, isAppend));
			StringBuilder sb = new StringBuilder();
			sb.append(host.getHostname() + " " + host.getIPAddr() + " " + host.getPort());
			bw.write(sb.toString());
			bw.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}finally { 
			   try{
			      if(bw!=null)
			    	  bw.close();
			   }catch(Exception ex){
			       System.out.println("Error in closing the BufferedWriter"+ex);
			   }
		}
	}
	
	public static void updateHost(Host host) {
		 
        try {
            if (!nets.isFile()) {
                System.out.println("Parameter is not an existing file");
                return;
            }
            //Construct the new file that will later be renamed to the original filename. 
            File tempFile = new File(nets.getAbsolutePath() + ".tmp");
            Files.copy(nets.toPath(), tempFile.toPath());
            BufferedReader br = new BufferedReader(new FileReader(tempFile));
            PrintWriter pw = new PrintWriter(new FileWriter(nets));
            String line;
            //Read from the original file and write to the new 
            //unless content matches data to be removed.
            while ((line = br.readLine()) != null) {
                if (!line.startsWith(host.getHostname())) {
                    pw.println(line);
                } else {
                	pw.println(host.getHostname() + " " + host.getIPAddr() + " " + host.getPort());
                }
                pw.flush();
            }
            pw.close();
            br.close();
 
            //Delete the original file
            if (!tempFile.delete()) {
                System.out.println("Could not delete file");
                return;
            }
 
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
	
	public static void deleteHost(Host host) {
		 
        try {
            if (!nets.isFile()) {
                System.out.println("Parameter is not an existing file");
                return;
            }
            //Construct the new file that will later be renamed to the original filename. 
            File tempFile = new File(nets.getAbsolutePath() + ".tmp");
            Files.copy(nets.toPath(), tempFile.toPath());
            BufferedReader br = new BufferedReader(new FileReader(tempFile));
            PrintWriter pw = new PrintWriter(new FileWriter(nets));
            String line;
            //Read from the original file and write to the new 
            //unless content matches data to be removed.
            while ((line = br.readLine()) != null) {
                if (!line.startsWith(host.getHostname())) {
                    pw.println(line);
                    pw.flush();
                }
            }
            pw.close();
            br.close();
 
            //Delete the original file
            if (!tempFile.delete()) {
                System.out.println("Could not delete file");
                return;
            }
 
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
	
	public static void deleteTuple(String datastr) {
		lock.lock();
        try {
            if (!tuples.isFile()) {
                System.out.println("Parameter is not an existing file");
                return;
            }
            //Construct the new file that will later be renamed to the original filename. 
            File tempFile = new File(tuples.getAbsolutePath() + ".tmp");
            Files.copy(tuples.toPath(), tempFile.toPath());
            BufferedReader br = new BufferedReader(new FileReader(tempFile));
            PrintWriter pw = new PrintWriter(new FileWriter(tuples));
            String line;
            boolean isDeleted = false;
            //Read from the original file and write to the new 
            //unless content matches data to be removed.
            while ((line = br.readLine()) != null) {
                if (!line.trim().equals(datastr) || isDeleted) {
                    pw.println(line);
                    pw.flush();
                } else {
                	isDeleted = true;
                }
            }
            pw.close();
            br.close();
 
            //Delete the original file
            if (!tempFile.delete()) {
                System.out.println("Could not delete file");
                return;
            }
 
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
        	lock.unlock();
        }
    }
	
	public static void cleanLocalFiles() {
		try{
    		if(nets.delete()){
    			System.out.println(nets.getName() + " file is deleted!");
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
    		if(tuples.delete()){
    			System.out.println(tuples.getName() + " file is deleted!");
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
    		if(backupmaps.delete()){
    			System.out.println(backupmaps.getName() + " file is deleted!");
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
    		if(name.delete()){
    			System.out.println(name.getName() + " folder is deleted!");
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
    	}catch(Exception e){
    		e.printStackTrace();
    	}
	}
	
	private static Tuple getTuple(String[] datastr){
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
			}
		}
		return tuple;
	}

	public static void clearTuple() {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(tuples));
			bw.write("");
		} catch (IOException e) {
			e.printStackTrace();
		}finally { 
			   try{
			      if(bw!=null)
			    	  bw.close();
			   }catch(Exception ex){
			       System.out.println("Error in closing the BufferedWriter"+ex);
			   }
		}	
	}
}
