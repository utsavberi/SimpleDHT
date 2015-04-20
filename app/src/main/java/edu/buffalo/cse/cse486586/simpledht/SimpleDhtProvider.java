package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();


    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    //port-><pre,succ>
    HashMap<String, NodePair> joinedNodes = new HashMap();

    class NodePair {

        String predecessor, successor;

        public NodePair(String predecessor, String successor) {
            this.predecessor = predecessor;
            this.successor = successor;
        }


    }

    //    ArrayList<String> joinedNodes = new ArrayList<>();
    static final int SERVER_PORT = 10000;
    public String predecessor_port = "";
    public String successor_port = "";
    String myPort;

    public void sendMesage(DHTMessage dhtMessage, String sendTo) {
        Log.d(TAG, "sending msg to " + sendTo);

        dhtMessage.send_to = sendTo;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dhtMessage);
        Log.d(TAG, "msg sent");
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d(TAG, "!!!!!!!!!!!!!!!!!!!!!!trying to delete" + myPort + ":" + selection);
        Log.d(TAG, selection + " " + successor_port + " " + predecessor_port + " " + myPort);
        if (selection.equals("\"@\"") || (selection.equals("\"*\"") &&
                (successor_port.isEmpty() || (predecessor_port.equals(myPort))))) {
            Log.d(TAG, "in @ query");
            for (File file : getContext().getFilesDir().listFiles()) {
                getContext().deleteFile(file.getName());
            }
            Log.d(TAG, "final cursor ");
        } else if (selection.equals("\"*\"")) {
            Log.d(TAG, "in * query");
//            String remotePort = remotePorts[0];
            String remotePort = successor_port;
            for (File file : getContext().getFilesDir().listFiles()) {
                getContext().deleteFile(file.getName());
            }
            DHTMessage dhtmsg = new DHTMessage();
            dhtmsg.from_port = myPort;
            dhtmsg.msgType = DHTMessage.MsgType.DELETE_ALL;
        }
        //selection !=@ && != *
        else {
            if (isCorrectNode(selection) == true || successor_port.isEmpty() || successor_port.equals(predecessor_port.equals(myPort))) {
                Log.d(TAG, "correct node");
                getContext().deleteFile(selection);
            } else {
                Log.d(TAG, "sending to successor");
                String remotePort = successor_port;
                DHTMessage queryMsg = new DHTMessage();
                queryMsg.msg = selection;
                queryMsg.msgType = DHTMessage.MsgType.DELETE;

            }

        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    private class ClientTask extends AsyncTask<DHTMessage, Void, Void> {
        @Override
        protected Void doInBackground(DHTMessage... msgs) {

            String remotePort = msgs[0].send_to;
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
                Log.d(TAG, "DHTmsg : sending msg " + msgs[0]);
                os.writeObject(msgs[0]);
                Log.d(TAG, "DHTmsg : msg sent to " + remotePort);
                socket.close();
            } catch (SocketException e) {
                Log.e(TAG, e.getLocalizedMessage());
                Log.e(TAG, "retrying");
                try {
                    Thread.sleep(1000);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[0]);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                    Log.e(TAG, e.getLocalizedMessage());
                }
                Log.e(TAG, "msg resent");


            } catch (IOException e) {
                e.printStackTrace();
//                Thread.sleep(1000);

            }

            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket clientSocket;
            try {
                int i = 0;
                while (true) {
                    clientSocket = serverSocket.accept();

                    ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(clientSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    Object object = null;
                    try {
                        object = ois.readObject();
                        if (object instanceof DHTMessage) {
                            DHTMessage dhtMessage = (DHTMessage) object;
                            Log.d(TAG, "received msg" + dhtMessage);
                            if (dhtMessage.msgType == DHTMessage.MsgType.JOIN) {
                                nodeJoin(dhtMessage);
                            } else if (dhtMessage.msgType == DHTMessage.MsgType.SET_PREDECESSOR) {
                                predecessor_port = dhtMessage.from_port;
                            } else if (dhtMessage.msgType == DHTMessage.MsgType.SET_SUCCESSOR) {
                                successor_port = dhtMessage.from_port;
                            } else if (dhtMessage.msgType == DHTMessage.MsgType.INSERT) {
                                ContentValues values = stringToCv(dhtMessage.cv_msg);
                                String key = values.getAsString("key");
                                String string = values.getAsString("value");
                                Log.d(TAG, "Insert rcvd:" + key + ":" + string);
                                insert(null, stringToCv(dhtMessage.cv_msg));
                            } else if (dhtMessage.msgType == DHTMessage.MsgType.QUERY) {
                                Log.d(TAG, "rcvd query msg, fetching cursor");
                                Cursor cur = query(null, null, dhtMessage.msg, null, null);
                                Log.d(TAG, "cursor filled converting to string");
                                String repQuery = cursorToString(cur);
                                Log.d(TAG, "converted to string " + repQuery);
                                Log.d(TAG, "replying");
                                out.println(repQuery);
                                Log.d(TAG, "reply done ");
                            } else if (dhtMessage.msgType == DHTMessage.MsgType.QUERY_ALL) {
                                String curr = cursorToString(query(null, null, "\"@\"", null, null));
                                if (dhtMessage.from_port.equals(successor_port) == false) {
                                    curr += queryAndWaitForReply(successor_port, dhtMessage);
                                } else {
                                    Log.d(TAG, "reached lastnode");
                                }
                                out.println(curr);
                            } else if (dhtMessage.msgType == DHTMessage.MsgType.DELETE) {
                                delete(null, dhtMessage.msg, null);
                            } else if (dhtMessage.msgType == DHTMessage.MsgType.DELETE_ALL) {
//                                String curr = cursorToString(query(null, null, "\"@\"", null, null));
                                delete(null,"\"@\"",null);
                                if (dhtMessage.from_port.equals(successor_port) == false) {
                                    sendMesage(dhtMessage, successor_port);
                                } else {
                                    Log.d(TAG, "reached lastnode");
                                }
                            } else {
                                Log.d(TAG, "rcvd illegal msg type");
                            }

                        } else {
                            Log.d(TAG, "received illegeal object");
                        }

                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }

//
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }


    }
    private String cursorToString(Cursor cursor) {
        Log.d(TAG, "converting cursor to string");
        StringBuilder sb = new StringBuilder();
        cursor.moveToFirst();
        while (cursor.isAfterLast() == false) {
            sb.append(cursor.getString(0));
            sb.append(",");
            sb.append(cursor.getString(1));
            sb.append("|");
            cursor.moveToNext();
        }
        Log.d(TAG, "Returning string:" + sb.toString());
        return sb.toString();
    }

    private ContentValues stringToCv(String cv_msg) {
        ContentValues cv = new ContentValues();
        if(cv_msg.trim().isEmpty()==true) return cv;
        cv.put("key", cv_msg.split(",")[0]);
        cv.put("value", cv_msg.split(",")[1]);
        return cv;
    }

    private void nodeJoin(DHTMessage join_dhtMessage) {
        try {
            Log.d(TAG, "hash join" + join_dhtMessage.from_port + ":" + genHash(getNodeIdFromPort(join_dhtMessage.from_port)));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (join_dhtMessage.from_port == myPort) {
            successor_port = myPort;
            predecessor_port = myPort;
        } else {
            String joinReq_from_port = join_dhtMessage.from_port;
            try {
                String joinfrom_NodeId = getNodeIdFromPort(joinReq_from_port);
                String joinFrom_NodeIdHash = genHash(joinfrom_NodeId);
                if (joinedNodes.size() == 1) {

                    Log.d(TAG, "in joined node size 1");
                    String newSuccessor = REMOTE_PORT0;
                    DHTMessage dhtMessage = new DHTMessage();
                    predecessor_port = joinReq_from_port;
                    successor_port = joinReq_from_port;
                    joinedNodes.put(REMOTE_PORT0, new NodePair(joinReq_from_port, joinReq_from_port));

                    updatePredSucc(REMOTE_PORT0, joinReq_from_port, DHTMessage.MsgType.SET_SUCCESSOR);
                    updatePredSucc(REMOTE_PORT0, joinReq_from_port, DHTMessage.MsgType.SET_PREDECESSOR);
                    joinedNodes.put(joinReq_from_port, new NodePair(REMOTE_PORT0, REMOTE_PORT0));
                } else {
                    String nodePosToInsert = REMOTE_PORT0;
                    int i = joinedNodes.size();
                    while (i > 0) {
                        String pre, wanna_join, curr;
                        curr = genHash(getNodeIdFromPort(nodePosToInsert));
                        pre = genHash(getNodeIdFromPort(joinedNodes.get(nodePosToInsert).predecessor));
                        wanna_join = genHash(getNodeIdFromPort(join_dhtMessage.from_port));
//                        if ((joinFrom_NodeIdHash.compareTo(genHash(getNodeIdFromPort(joinedNodes.get(nodePosToInsert).predecessor))) > 0
//                                && joinFrom_NodeIdHash.compareTo(genHash(getNodeIdFromPort(nodePosToInsert))) <= 0)||(genHash(getNodeIdFromPort(joinedNodes.get((nodePosToInsert)).predecessor)).compareTo(getNodeIdFromPort(nodePosToInsert))>0)) {
//                        if(joinfrom_NodeId.compareTo(joinedNodes.get((nodePosToInsert)).predecessor)>0 && joinfrom_NodeId.compareTo(nodePosToInsert)<=0){
//                            break;
                        //id.compareTo(pred_id) > 0)
//                        || ((my_id.compareTo(pred_id) < 0) && id.compareTo(my_id) < 0)
                        if ((wanna_join.compareTo(pre) > 0 && wanna_join.compareTo(curr) < 0) ||
                                ((pre.compareTo(curr) > 0)&&(wanna_join.compareTo(curr)<0 || wanna_join.compareTo(pre)>0))) {
                            break;
                        } else nodePosToInsert = joinedNodes.get(nodePosToInsert).successor;
                        i--;
                    }

                    if (i == 0) {
                        nodePosToInsert = joinedNodes.get(nodePosToInsert).successor;
                    }

                    String nodePos_pre = joinedNodes.get(nodePosToInsert).predecessor;

                    //update(updatedportno,sendmsgto,msgtype)
                    updatePredSucc(joinReq_from_port, nodePosToInsert, DHTMessage.MsgType.SET_PREDECESSOR);
                    updatePredSucc(joinReq_from_port, nodePos_pre, DHTMessage.MsgType.SET_SUCCESSOR);

                    updatePredSucc(nodePosToInsert, joinReq_from_port, DHTMessage.MsgType.SET_SUCCESSOR);
                    updatePredSucc(nodePos_pre, joinReq_from_port, DHTMessage.MsgType.SET_PREDECESSOR);


                    NodePair update = new NodePair(joinReq_from_port, joinedNodes.get(nodePosToInsert).successor);
                    joinedNodes.put(nodePosToInsert, update);
                    NodePair update2 = new NodePair(joinedNodes.get(nodePos_pre).predecessor, joinReq_from_port);
                    joinedNodes.put(nodePos_pre, update2);
                    joinedNodes.put(joinReq_from_port, new NodePair(nodePos_pre, nodePosToInsert));
                }

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Log.d(TAG, "joined nodes" + printJoinedNodes());
        }

    }

    private void updatePredSucc(String updatedPortNum, String nodeToUpdate, DHTMessage.MsgType msgType) {
        DHTMessage dhtMessage = new DHTMessage();
        dhtMessage.from_port = updatedPortNum;
        dhtMessage.msgType = msgType;// DHTMessage.MsgType.SET_PREDECESSOR;
        sendMesage(dhtMessage, nodeToUpdate);
    }


    private String printJoinedNodes() {
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

    private String getNodeIdFromPort(String port) {
        return Integer.toString(Integer.parseInt(port) / 2);

    }

    @Override
    public boolean onCreate() {

//        Log.d(TAG,"it has begun");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        if (myPort.equals(remotePorts[0])) {
            successor_port = REMOTE_PORT0;
            predecessor_port = REMOTE_PORT0;
            joinedNodes.put(REMOTE_PORT0, new NodePair(REMOTE_PORT0, REMOTE_PORT0));
        }


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket " + e.getLocalizedMessage());

        }
        if (myPort.equals(REMOTE_PORT0) == false) {
            DHTMessage joinMessage = new DHTMessage();
            joinMessage.from_port = myPort;
            joinMessage.msgType = DHTMessage.MsgType.JOIN;
            sendMesage(joinMessage, REMOTE_PORT0);
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.d(TAG, "!!!!!!!!!!!!!!!!!!!!!!trying to query" + myPort + ":" + selection);
        MatrixCursor cur;
        cur = new MatrixCursor(new String[]{"key", "value"});
        Log.d(TAG, "started querying");
        Log.d(TAG, selection + " " + successor_port + " " + predecessor_port + " " + myPort);
        if (selection.equals("\"@\"") || (selection.equals("\"*\"") &&
                (successor_port.isEmpty() || (predecessor_port.equals(myPort))))) {
            Log.d(TAG, "in @ query");
            for (File file : getContext().getFilesDir().listFiles()) {
                cur.addRow(new Object[]{file.getName(), readFile(file.getName())});
            }
            Log.d(TAG, "final cursor " + cur);
            return cur;
        } else if (selection.equals("\"*\"")) {
            Log.d(TAG, "in * query");
//            String remotePort = remotePorts[0];
            String remotePort = successor_port;
            for (File file : getContext().getFilesDir().listFiles()) {
                cur.addRow(new Object[]{file.getName(), readFile(file.getName())});
            }
            DHTMessage dhtmsg = new DHTMessage();
            dhtmsg.from_port = myPort;
            dhtmsg.msgType = DHTMessage.MsgType.QUERY_ALL;
            convertAndAppendToCur(cur, queryAndWaitForReply(remotePort, dhtmsg));
            Log.d(TAG,"received cursor");
            Log.d(TAG,cursorToString(cur));
            return cur;
        }
        //selection !=@ && != *
        else {
            if (isCorrectNode(selection) == true || successor_port.isEmpty() || successor_port.equals(predecessor_port.equals(myPort))) {
                Log.d(TAG, "correct node");
                cur.addRow(new Object[]{selection, readFile(selection)});
                return cur;
            } else {
                Log.d(TAG, "sending to successor");
                String remotePort = successor_port;
                DHTMessage queryMsg = new DHTMessage();
                queryMsg.msg = selection;
                queryMsg.msgType = DHTMessage.MsgType.QUERY;
                convertAndAppendToCur(cur, queryAndWaitForReply(remotePort, queryMsg));
                return cur;

            }

        }
    }

    private String queryAndWaitForReply(String remotePort, DHTMessage dhtMsg) {
        Socket socket = null;
        Log.d(TAG, "inquery and wait for reply " + remotePort + "  " + dhtMsg);
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort));
            PrintWriter out =
                    new PrintWriter(socket.getOutputStream(), true);
            ObjectOutputStream ois = new ObjectOutputStream(socket.getOutputStream());
            BufferedReader in =
                    new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
//            DHTMessage dhtmsg = new DHTMessage();
//            dhtmsg.msgType = dhtMsg;
            ois.writeObject(dhtMsg);
            Log.d(TAG, "msg sent to successor waiting for reply");
            String reply = in.readLine();
            Log.d(TAG, "rcvd reply " + reply);
//            socket.close();
            return reply;
        } catch (IOException e) {
            e.printStackTrace();
        }
        Log.d(TAG, "returning empty");
        return "";
    }

    private String readFile(String filename) {
        String tmp = "";
        BufferedReader rdr = null;
        try {
            Log.d(TAG, "in file" + filename);
            rdr = new BufferedReader(new InputStreamReader(getContext().openFileInput(filename)));
            Log.d(TAG, "read file done" + filename);
            tmp = rdr.readLine();
            Log.d(TAG, "added file to cursor");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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

    private void convertAndAppendToCur(MatrixCursor cur, String s) {
        if(s.trim().isEmpty()) return;
        String[] rows = s.split("\\|");
        for (String row : rows) {
            String[] keyValue = row.split(",");
            cur.addRow(new Object[]{keyValue[0], keyValue[1]});
        }

    }

    private boolean isCorrectNode(String key) {
        Log.d(TAG, "checking for correct node");
        Log.d(TAG, successor_port + " " + predecessor_port + " " + myPort);
        if (successor_port.isEmpty()) {
            Log.d(TAG, "successor is empty returning true");
            return true;
        }
        if ((predecessor_port.equals(myPort))) {
            return true;
        }
        try {
            String id = genHash(key);
            String pred_id = genHash(getNodeIdFromPort(predecessor_port));
            String my_id = genHash(getNodeIdFromPort(myPort));
            boolean f = true;
//            if (myPort.equals(remotePorts[0])) {
//                f = false;
//                for (String e : joinedNodes.keySet()) {
//                    if (id.compareTo(genHash(e)) <= 0)
//                        f = true;
//                }
//                if (f == false) return true;
//            }

            if ((id.compareTo(pred_id) > 0 && id.compareTo(my_id) <= 0) || ((my_id.compareTo(pred_id) < 0) && id.compareTo(pred_id) > 0)
                    || ((my_id.compareTo(pred_id) < 0) && id.compareTo(my_id) < 0)) {
                return true;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return false;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(TAG, "inerting value" + values);
        Log.d(TAG, "!!!!!!!!!!!!!!!!!!!!!!trying to insert" + myPort);
        String key = values.getAsString("key");
        String string = values.getAsString("value");
        Log.d(TAG, "succc:" + successor_port);
        if (isCorrectNode(key) == true) {
            Log.d(TAG, "CorrectNode");
            FileOutputStream outputStream;
            try {
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
        } else {
            Log.d(TAG, "not correctNode");
            DHTMessage insertDhtMessage = new DHTMessage();
            insertDhtMessage.msgType = DHTMessage.MsgType.INSERT;
            insertDhtMessage.from_port = myPort;
            insertDhtMessage.cv_msg = cvtoString(values);
            sendMesage(insertDhtMessage, successor_port);
        }
        return uri;
    }

    private String cvtoString(ContentValues values) {
        String s = "";
        try{
        s += values.getAsString("key") + ",";
        s += values.getAsString("value");}catch (IndexOutOfBoundsException ex){Log.e(TAG, ex.getLocalizedMessage());ex.printStackTrace();}
        return s;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
