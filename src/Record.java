/**
 * Value object of tuple. It contains actual value and value type
 * 
 * @author Liang Xia
 */
public class Record {
	public enum Type{
		STRING, INTEGER, FLOAT
	}
	private Type type;
	private String value;
	
	public Record(Type type, String value){
		this.type = type;
		this.value = value;
	}
	
	public Type getType(){
		return type;
	}
	
	public String getValue(){
		return value;
	}
	
	public void setType(Type type){
		this.type = type;
	}
	
	public void setValue(String	value){
		this.value = value;
	}
}
