package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.Cursor;



import java.io.Serializable;

/**
 * Created by utsav on 4/1/15.
 */
public class DHTMessage implements Serializable {

    public DHTMessage(){visit0count = 0;}
    int visit0count;
    public String cv_msg;
    public String msg;
    public Cursor cur_msg;

    public enum MsgType{
        SET_PREDECESSOR, SET_SUCCESSOR, INSERT, QUERY, QUERY_ALL, DELETE, DELETE_ALL, JOIN
    }

    MsgType msgType;
    String from_port;
    String send_to;

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("msgtype:");
        sb.append(msgType.toString()+"\n");
        sb.append("fromport:"+from_port+"\n");
        sb.append("sendto:"+send_to+"\n");
        return sb.toString();
    }
}
