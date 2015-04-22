package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.util.Log;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by utsav on 4/20/15.
 */
public class MyUtils {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    public static String cursorToString(Cursor cursor) {
        Log.d(TAG, "converting cursor to string");
        StringBuilder sb = new StringBuilder();
        cursor.moveToFirst();
        while (!cursor.isAfterLast()) {
            sb.append(cursor.getString(0));
            sb.append(",");
            sb.append(cursor.getString(1));
            sb.append("|");
            cursor.moveToNext();
        }
        Log.d(TAG, "Returning string:" + sb.toString());
        return sb.toString();
    }

    public static ContentValues stringToCv(String cv_msg) {
        ContentValues cv = new ContentValues();
        if(cv_msg.trim().isEmpty()) return cv;
        cv.put("key", cv_msg.split(",")[0]);
        cv.put("value", cv_msg.split(",")[1]);
        return cv;
    }
    public static String getNodeIdFromPort(String port) {
        return Integer.toString(Integer.parseInt(port) / 2);

    }

    public static String readFile(FileInputStream filename) {
        String tmp = "";
        BufferedReader rdr = null;
        try {
            Log.d(TAG, "in file" + filename);
            rdr = new BufferedReader(new InputStreamReader(filename));
            Log.d(TAG, "read file done" + filename);
            tmp = rdr.readLine();
            Log.d(TAG, "added file to cursor");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (rdr != null) {
                try {
                    rdr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return tmp;
    }

    public static void convertAndAppendToCur(MatrixCursor cur, String s) {
        if(s.trim().isEmpty()) return;
        String[] rows = s.split("\\|");
        for (String row : rows) {
            String[] keyValue = row.split(",");
            cur.addRow(new Object[]{keyValue[0], keyValue[1]});
        }

    }

    public static String cvtoString(ContentValues values) {
        String s = "";
        try{
            s += values.getAsString("key") + ",";
            s += values.getAsString("value");}catch (IndexOutOfBoundsException ex){Log.e(TAG, ex.getLocalizedMessage());ex.printStackTrace();}
        return s;
    }

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static String printJoinedNodes(HashMap<String,NodePair> joinedNodes) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, NodePair> e : joinedNodes.entrySet()) {
            sb.append(e.getKey());
            sb.append("->");
            sb.append(e.getValue().predecessor);
            sb.append(":");
            sb.append(e.getValue().successor);
            sb.append("\n");
        }
        return sb.toString();
    }
}
