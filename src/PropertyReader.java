/**
 * Property reader loads the properties file during the initialization of the program.
 * @author Liang Xia
 */
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyReader {
	Map<String, String> result = new HashMap<>();
	InputStream inputStream;
 
	public Map<String, String> getHosts(){
 
		try {
			Properties prop = new Properties();
			String propFileName = "hosts.properties";
 
			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
 
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
			
			for(Object key: prop.keySet()) {
				String host = (String) key;
				String ip = prop.getProperty(host);
				result.put(host, ip);
			}
 

		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
}
