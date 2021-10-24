package org.utilities;
import org.apache.hadoop.io.*;

public class StringPair{
    String first;
    String second;
    char seperator;

    public StringPair(Text source_pair, char seperator){
        this(source_pair.toString(), seperator);
    }

    public StringPair(String source_pair, char seperator){
        String[] parts = source_pair.split(Character.toString(seperator));
        this.first = new String(parts[0]);
        this.second = new String(parts[1]);
        this.seperator = seperator;
    }
    public StringPair(String first, char seperator, String second){
        this.first = new String(first);
        this.second = new String(second);
        this.seperator = seperator;
    }
    public StringPair(Text first, char seperator, Text second){
        this(first.toString(), seperator, second.toString());
    }
    public static StringPair combineParts(String part_a,  char seperator, String part_b){
        boolean order_is_a_b = (part_b.charAt(0) == seperator);
        String string_seperator = Character.toString(seperator);
        return new StringPair(
                (order_is_a_b?part_a:part_b).split(string_seperator)[0],
                seperator,
                (order_is_a_b?part_b:part_a).split(string_seperator)[1]);
    }
    public static StringPair combineParts(Text part_a, char seperator, Text part_b){
        return combineParts(part_a.toString(), seperator, part_b.toString());
    }

    /**
     * returns a copy of the part of the string pair which is not equal to the parameter 'value'.
     * if neither part equals the parameter, copies the first part.
     */
    public String copyOtherPart(String value){
        return new String(first.equals(value) ? second : first);
    }
    public String toString(){
        return new String(this.first+Character.toString(seperator)+this.second);
    }
    public Text toText(){
        return new Text(this.first+Character.toString(seperator)+this.second);
    }
    public String copyFirst(){
        return new String(this.first);
    }
    public String copySecond(){
        return new String(this.second);
    }
    public char getSeperator(){
        return seperator;
    }
}