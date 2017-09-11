/**
 * Tuple object which stores the list of values.
 * 
 * @author Liang Xia
 */
import java.util.ArrayList;
import java.util.List;

public class Tuple {
	
	List<Record> records;
	
	public Tuple() {
		records = new ArrayList<>();
	}
	
	public void addRecord(Record r) {
		records.add(r);
	}
	
	public void clear(){
		records.clear();
	}
	
	public List<Record> getRecords(){
		return records;
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
     	for(Record r : records){
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
	
}
