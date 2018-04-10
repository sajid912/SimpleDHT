package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.PriorityQueue;
import java.util.Random;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.MediaStore;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import static android.content.ContentValues.TAG;

//Edited by sajid again
public class SimpleDhtProvider extends ContentProvider {


    private static String EMULATOR_0_HASH = "";
    private static String EMULATOR_1_HASH = "";
    private static String EMULATOR_2_HASH = "";
    private static String EMULATOR_3_HASH = "";
    private static String EMULATOR_4_HASH = "";

    private Node my_node;
    private String MY_PREDECESSOR_PORT;
    private String MY_SUCCESSOR_PORT;
    private boolean ringActive = false;
    private boolean globalFlag = false;
    private String globalValue = "";
    private Object valueLock = new Object();
    private ArrayList<Node> ringList = new ArrayList<Node>();
    private ArrayList<String> allDataQueryReplyList = new ArrayList<String>();
    private int allDataQueryReplyCount = 0;
    private ArrayList<String> activePorts = new ArrayList<String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.v("delete", selection);

        try {

            if(selection.equals("*")) // delete all key value pairs in entire DHT
            {
                for(File file: getContext().getFilesDir().listFiles())
                {
                    file.delete();
                }

            }
            else if(selection.equals("@")) // delete key value pairs in the local node
            {
                for(File file: getContext().getFilesDir().listFiles())
                {
                   file.delete();
                }

            }
            else // other cases
            {
                 getContext().getFileStreamPath(selection).delete();
            }

        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Log.v("insert", values.toString());
        String key = values.getAsString("key");
        String value = values.getAsString("value");

        insertValues(key, value);
        return uri;
    }

    private String getPredecessorId()
    {
        int pre_p = Integer.parseInt(MY_PREDECESSOR_PORT);
        int pre_id = pre_p/2;
        String preIdStr = String.valueOf(pre_id);
        return genHash(preIdStr);
    }

    private void insertValues(String key, String value)
    {
        boolean forward = false;

        if(ringActive)
        {
            String my_nodeId = my_node.getNodeId();
            String my_preId = getPredecessorId();

            checkValues(key);
            Log.d(TAG,"Key:"+key);
            String hashedKey = genHash(key);

            int comparision = hashedKey.compareTo(my_nodeId);
            Log.d(TAG,"Comparision of hashedKey and my node id:"+comparision);

            forward = !saveLocally(hashedKey, my_preId, my_nodeId);

        }

        if(forward)
        {
            Log.d(TAG,"Forwarding the request");
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MY_SUCCESSOR_PORT,
                    constructDataObject(key, value), Constants.DATA_FORWARD_REQUEST);
        }
        else
        {
            Log.d(TAG,"Trying to save locally");
            try {
                value = value + "\n";
                FileOutputStream outputStream;
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();

            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
        }

    }

    private boolean saveLocally(String hashedKey, String my_preId, String my_nodeId)
    {
        boolean save = true;

        if(hashedKey.compareTo(my_nodeId)>0)
        {
            int c1= my_preId.compareTo(my_nodeId);
            int c2= hashedKey.compareTo(my_preId);
            Log.d(TAG, "Comparision of pred and my node id:"+c1);
            Log.d(TAG, "Comparision of hashedKey and pred:"+c2);

            if(my_preId.compareTo(my_nodeId)>0 && hashedKey.compareTo(my_preId)>0) // My pre is greater than me && key is greater than my pre
            {
                // save the key locally
                Log.d(TAG, "Higher than large node, save it");

            }
            else
            {
                save = false; // Forward to successor
                Log.d(TAG,"Key greater than me, forward to successor");
            }

        } else if(hashedKey.compareTo(my_nodeId)<0)
        {
            int c1= my_preId.compareTo(my_nodeId);
            int c2= hashedKey.compareTo(my_preId);
            Log.d(TAG, "Comparision of pred and my node id:"+c1);
            Log.d(TAG, "Comparision of hashedKey and pred:"+c2);

            if(hashedKey.compareTo(my_preId)>0)
            {
                // Store the value locally
                Log.d(TAG, "Key less than me, greater than pred, save it");

            }
            else if(my_preId.compareTo(my_nodeId)>0) // My pre is greater than me && key is less than me
            {
                // save locally
                Log.d(TAG, "Smaller than small node, so save it");

            }
            else
            {
                save = false;
                Log.d(TAG,"Key smaller than me and my pred, so forward");
            }

        }

      return save;
    }

    private String constructDataObject(String key, String value)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 3); // 3 stands for data forward among nodes
            jsonObject.put(Constants.DATA_ORIGIN_PORT, my_node.getPortNo());
            jsonObject.put(Constants.KEY, key); // port no of predecessor
            jsonObject.put(Constants.VALUE, value); // port no of successor
            jsonObject.put(Constants.MY_PORT, my_node.getPortNo());
            //jsonObject.put(Constants.COUNT, count);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private void queryValues(String key, String queryObject)
    {
        boolean forward = false;

        if(ringActive)
        {
            String my_nodeId = my_node.getNodeId();
            String my_preId = getPredecessorId();

            //checkValues(key);
            Log.d(TAG,"Key:"+key);
            String hashedKey = genHash(key);

            //int comparision = hashedKey.compareTo(my_nodeId);
            //Log.d(TAG,"Comparision of hashedKey and my node id:"+comparision);

            forward = !saveLocally(hashedKey, my_preId, my_nodeId);

        }

        if(forward)
        {
            Log.d(TAG,"Forwarding the query request");
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MY_SUCCESSOR_PORT,
                    queryObject, Constants.DATA_QUERY_REQUEST);
        }
        else
        {
            Log.d(TAG,"I have the key, will send query result to requestor");

            try {
                JSONObject jsonObject = new JSONObject(queryObject);
                String replyPort = jsonObject.getString(Constants.QUERY_ORIGIN_PORT);
                new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replyPort,
                        constructQueryReplyObject(key, getFileContentFromName(key)), Constants.DATA_QUERY_REPLY);

            } catch (JSONException e) {
                e.printStackTrace();
            }

        }

    }

    private String queryAllValues()
    {

        //Log.d(TAG,"replying all data query");
        StringBuilder allFileContent = new StringBuilder("");

        for(File file: getContext().getFilesDir().listFiles())
        {
            String fileName = file.getName();
            String fileValue = getFileContentFromName(fileName);
            allFileContent.append(fileName+Constants.KEY_VALUE_SEPARATOR+fileValue+Constants.TEXT_SEPARATOR);
        }

        String content = allFileContent.toString();

        if(content.isEmpty())
            content = Constants.TEXT_SEPARATOR; // Sending only text separator when no files are present
        return constructAllDataQueryReplyObject(content);
//        new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryOriginPort,
//                constructAllDataQueryReplyObject(content), Constants.ALL_DATA_QUERY_REPLY);

    }
    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Log.v(TAG, "Content provider created!!");
        onNodeStart(getEmulatorId());
        return false;
    }

    private void checkValues(String key)
    {
        EMULATOR_0_HASH = genHash("5554");
        EMULATOR_1_HASH = genHash("5556");
        EMULATOR_2_HASH = genHash("5558");
        EMULATOR_3_HASH = genHash("5560");
        EMULATOR_4_HASH = genHash("5562");
        String hash = genHash(key);

        Log.d(TAG, hash.compareTo(EMULATOR_4_HASH)+" check0");
        Log.d(TAG, hash.compareTo(EMULATOR_1_HASH)+" check1");
        Log.d(TAG, hash.compareTo(EMULATOR_0_HASH)+" check2");
        Log.d(TAG, hash.compareTo(EMULATOR_2_HASH)+" check3");
        Log.d(TAG, hash.compareTo(EMULATOR_3_HASH)+" check4");
    }

    private void addAllLocalFilesToCursor(MatrixCursor matrixCursor)
    {
        // Add the local content
        for(File file: getContext().getFilesDir().listFiles())
        {
            String fileName = file.getName();
            String fileValue = getFileContentFromName(fileName);
            matrixCursor.addRow(new Object[]{fileName, fileValue});
        }
    }

    private MatrixCursor handleAllDataQueryLocal(MatrixCursor matrixCursor)
    {
        try {

            if(ringActive)
            {
                if(my_node.getPortNo().equals(Constants.EMULATOR0_PORT))
                {
                    // 5554 need not query, it has all active ports locally in ringList

                    activePorts.clear();
                    for(int i=0;i<ringList.size();i++)
                        activePorts.add(ringList.get(i).getPortNo());

                    Log.d(TAG,"You have activePorts locally, length is"+activePorts.size());
                }
                else if(activePorts.size() == 0) // You dont have active ports, query 5554
                {
                    synchronized (valueLock) {
                        while ( globalFlag != true ) {

                            // get the list of all nodes in the ring from 5554 before you ask for their values

                            if(!my_node.getPortNo().equals(Constants.EMULATOR0_PORT))
                            {
                                new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                        Constants.EMULATOR0_PORT, constructNodeListQueryObject(), Constants.NODE_LIST_QUERY_REQUEST);
                            }
                            valueLock.wait();

                        }
                        // value is now true
                        globalFlag = false;
                        Log.d(TAG,"Got all activePorts from 5554, length is"+activePorts.size());
                }

                }

                synchronized (valueLock)
                {
                    while ( globalFlag != true ) {

                        // Now you have activePorts list
                        for(String port: activePorts) // change this to dynamic ports
                        {
                            if(!port.equals(my_node.getPortNo()))
                            {
                                new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                        port, constructAllDataQueryObject(), Constants.ALL_DATA_QUERY_REQUEST);
                            }
                        }

                        valueLock.wait();

                    }

                    // value is now true

                    globalFlag = false;

                    // Add the remote content
                    for(String s: allDataQueryReplyList)
                    {
                        String a[] = s.split(Constants.KEY_VALUE_SEPARATOR);
                        matrixCursor.addRow(new Object[]{a[0], a[1]});
                    }

                    // Add the local content
                    addAllLocalFilesToCursor(matrixCursor);

                    allDataQueryReplyCount = 0;
                    allDataQueryReplyList.clear(); // reset the values

                }


            } else {
                // only the local content
                addAllLocalFilesToCursor(matrixCursor);
            }

        } catch ( InterruptedException x ) {
            Log.d(TAG,"interrupted while waiting");
        }

        return matrixCursor;
    }

    private MatrixCursor handleOtherQueryCases(MatrixCursor matrixCursor, String selection)
    {
        if(ringActive)
        {
            String hashedKey = genHash(selection);
            String my_preId = getPredecessorId();

            boolean isFileLocal = saveLocally(hashedKey, my_preId, my_node.getNodeId());

            if(isFileLocal)
            {
                return returnCursorFromName(selection, matrixCursor);
            }
            else // ask the successor for file
            {
                try {
                    synchronized (valueLock) {
                        while ( globalFlag != true ) {

                            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                    MY_SUCCESSOR_PORT, constructDataQueryObject(selection), Constants.DATA_QUERY_REQUEST);

                            valueLock.wait();

                        }
                        // value is now true

                        globalFlag = false;
                        matrixCursor.addRow(new Object[]{selection, globalValue});
                        return matrixCursor;


                    }
                } catch ( InterruptedException x ) {
                    Log.d(TAG,"interrupted while waiting");
                }
            }
        } else {
            return returnCursorFromName(selection, matrixCursor);
        }

        return matrixCursor;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        Log.v("query", selection);

        try {

            if(selection.equals("*")) // return all key value pairs in entire DHT
            {

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                return handleAllDataQueryLocal(matrixCursor);

            }
            else if(selection.equals("@")) // return key value pairs in the local node
            {
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

                for(File file: getContext().getFilesDir().listFiles())
                {
                    String fileName = file.getName();
                    String fileValue = getFileContentFromName(fileName);
                    matrixCursor.addRow(new Object[]{fileName, fileValue});
                }

                return matrixCursor;
            }
            else // other cases
            {

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                return handleOtherQueryCases(matrixCursor, selection);
            }

        } catch (Exception e) {
            Log.e(TAG, "File write failed");
            e.printStackTrace();
        }

        return null;
    }

    private MatrixCursor returnCursorFromName(String fileName, MatrixCursor cursor)
    {
        String value = getFileContentFromName(fileName);
        cursor.addRow(new Object[]{fileName, value});
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input){
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
      return "";
    }


    private String getFileContentFromName(String fileName)
    {

        try {
            FileInputStream inputStream = getContext().openFileInput(fileName);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String value = bufferedReader.readLine();
            Log.d(TAG, "Value is:" + value);
            bufferedReader.close();
            return value;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
       return "";
    }

    private String getEmulatorId()
    {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr)));
    }

    public String getPortNumber(String emulatorId)
    {
        int a = Integer.parseInt(emulatorId);
        int port_no = 2*a;
        return String.valueOf(port_no);
    }

    private void onNodeStart(String emulatorId)
    {
        try {
            //ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            ServerSocket serverSocket = new ServerSocket(); // <-- create an unbound socket first
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(Constants.SERVER_PORT));
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
            return;
        }

        my_node = new Node(genHash(emulatorId), getPortNumber(emulatorId));
        ringList.add(my_node);

        if(!my_node.getPortNo().equals(Constants.EMULATOR0_PORT))
        {
          //  All emulators except 5554 will request 5554 to join DHT
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.EMULATOR0_PORT, constructNodeJoinObject(), Constants.JOIN_REQUEST);

        }
    }

    private String constructDataQueryObject(String key)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 4); // 4 stands for data query request
            jsonObject.put(Constants.KEY, key);
            jsonObject.put(Constants.QUERY_ORIGIN_PORT, my_node.getPortNo()); // port no of query node

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructQueryReplyObject(String key, String value)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 5); // 5 stands for data query reply
            jsonObject.put(Constants.KEY, key);
            jsonObject.put(Constants.VALUE, value);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructAllDataQueryObject()
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 6); // 6 stands for all data query request
            jsonObject.put(Constants.QUERY_ORIGIN_PORT, my_node.getPortNo()); // port no of query node

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructAllDataQueryReplyObject(String content)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 7); // 7 stands for all data query reply
            jsonObject.put(Constants.ALL_DATA_QUERY_CONTENT, content); // query content

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructNodeListQueryObject()
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 8); // 8 stands for node list query
            jsonObject.put(Constants.MY_PORT, my_node.getPortNo()); // node List placeholder
            jsonObject.put(Constants.NODE_LIST_CONTENT, ""); // node List placeholder

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private String constructNodeJoinObject()
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 0); // 0 stands for DHT join request
            jsonObject.put(Constants.PREDECESSOR_PORT, "0"); // port no of predecessor
            jsonObject.put(Constants.SUCCESSOR_PORT, "0"); // port no of successor
            jsonObject.put(Constants.MY_PORT, my_node.getPortNo());

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private class clientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try {

                if(!msgs[0].equals(my_node.getPortNo()))
                {
                    Socket socket = new Socket();

                    socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{(byte) 10, (byte) 0, (byte) 2, (byte) 2}),
                            Integer.parseInt(msgs[0])));

                    String msgToSend = msgs[1];

                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgToSend);

                    Log.d(TAG, "clientTask to port" + msgToSend + " : " + msgs[0]);

                    if(msgs[2].equals(Constants.JOIN_REQUEST) || msgs[2].equals(Constants.ALL_DATA_QUERY_REQUEST)
                            || msgs[2].equals(Constants.NODE_LIST_QUERY_REQUEST))
                    {
                        InputStream inputStream = socket.getInputStream();
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                        String reply = bufferedReader.readLine();
                        Log.d(TAG, "Reply status:" + reply);
                        if(msgs[2].equals(Constants.JOIN_REQUEST))
                            handleJoinRequestReply(reply);
                        else if(msgs[2].equals(Constants.ALL_DATA_QUERY_REQUEST))
                            handleAllDataQueryReply(reply);
                        else if(msgs[2].equals(Constants.NODE_LIST_QUERY_REQUEST))
                            handleNodeListQueryReply(reply);
                        bufferedReader.close();
                    }

                    out.close();
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (SocketException e) {
                Log.e(TAG, "Socket Exception, " + msgs[0] + " has failed");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            } catch (Exception e) {
                Log.e(TAG, "General exception");
                e.printStackTrace();
            }

            return null;
        }

        protected void handleJoinRequestReply(String reply)
        {
            try {
                JSONObject jsonObject = new JSONObject(reply);
                int status = jsonObject.getInt(Constants.STATUS);
                if(status == 1)
                {
                    ringActive = true;
                    MY_PREDECESSOR_PORT = jsonObject.getString(Constants.PREDECESSOR_PORT);
                    MY_SUCCESSOR_PORT = jsonObject.getString(Constants.SUCCESSOR_PORT);
                }


            } catch (JSONException e) {
                e.printStackTrace();
            }
        }


        protected void handleAllDataQueryReply(String object)
        {
            try {

                Log.d(TAG, "All Data query reply:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String content = jsonObject.getString(Constants.ALL_DATA_QUERY_CONTENT);

                String[] allFileContent = content.split(Constants.TEXT_SEPARATOR);

                for(String s: allFileContent)
                {
                    if(!s.isEmpty())
                        allDataQueryReplyList.add(s); // add each key value pair to the list
                }
                allDataQueryReplyCount++;

                if(allDataQueryReplyCount == (activePorts.size()-1)) // Received content from all active emulators
                {
                    synchronized ( valueLock ) {
                        globalFlag = true;
                        valueLock.notifyAll();  // notifyAll() might be safer...
                    }
                }


            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void handleNodeListQueryReply(String object)
        {
            try {

                Log.d(TAG, "Node list query reply:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String content = jsonObject.getString(Constants.NODE_LIST_CONTENT);

                String[] allPorts = content.split(Constants.TEXT_SEPARATOR);

                activePorts.clear(); // clear previous values
                for(String s: allPorts)
                {
                    if(!s.isEmpty())
                        activePorts.add(s); // add each port to the activePorts
                }

                synchronized ( valueLock ) {
                    globalFlag = true;
                    valueLock.notifyAll();  // notifyAll() might be safer...
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }


    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private Node remoteNode;
        private Node predecessorNode;
        private Node successorNode;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String message;
            Log.d(TAG, "Server task");

            while (true) {
                try {
                    Socket socket = serverSocket.accept();

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    message = bufferedReader.readLine();

                    Log.d(TAG,"Message received:"+message);
                    OutputStream outputStream = socket.getOutputStream();
                    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

                    JSONObject jsonObject = new JSONObject(message);
                    int status = jsonObject.getInt(Constants.STATUS);
                    if (status == 0) {
                        String reply = handleNodeJoinRequest(message);
                        Log.d(TAG,"Status is 0");
                        bufferedWriter.write(reply);
                        publishProgress(Constants.UPDATE_NEIGHBORS_LOCAL);

                    } else if(status == 2){
                        // Neighbor update msg
                        Log.d(TAG,"Status is 2");
                        publishProgress(Constants.UPDATE_NEIGHBORS_REMOTE, message);

                    } else if(status == 3){
                        // Data forward request
                        Log.d(TAG,"Status is 3");
                        publishProgress(Constants.DATA_FORWARD_REQUEST, message);

                    } else if(status == 4){

                        Log.d(TAG,"Status is 4");
                        publishProgress(Constants.DATA_QUERY_REQUEST, message);
                    } else if(status == 5){

                        Log.d(TAG,"Status is 5");
                        publishProgress(Constants.DATA_QUERY_REPLY, message);
                    } else if(status == 6){

                        Log.d(TAG,"Status is 6");
                        String reply = handleAllDataQueryRequest(message);
                        bufferedWriter.write(reply);
                        //publishProgress(Constants.ALL_DATA_QUERY_REQUEST, message);
                    }
//                    else if(status == 7){
//
//                        Log.d(TAG,"Status is 7");
//                        publishProgress(Constants.ALL_DATA_QUERY_REPLY, message);
//                    }
                    else if (status == 8) {
                        String reply = handleNodeListQueryRequest(message);
                        Log.d(TAG,"Status is 8");
                        bufferedWriter.write(reply);
                    }

                    bufferedWriter.flush();
                    bufferedWriter.close();

                    bufferedReader.close();
                    socket.close();
                } catch (SocketTimeoutException e) {
                    Log.d(TAG, "Socket time out occurred");
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                //return null;
            }
        }

        protected void onProgressUpdate(String... strings) {

            //Log.d(TAG, "Received:" + strings[0] + " My port:" + myPort);
            if(strings[0].equals(Constants.UPDATE_NEIGHBORS_LOCAL))
            {
                ringActive = true;
                Log.v(TAG,"Updating neighbors locally");
                updateNeighbours(remoteNode, predecessorNode, successorNode);
            }
            else if(strings[0].equals(Constants.UPDATE_NEIGHBORS_REMOTE))
            {
                Log.v(TAG,"Updating neighbors remotely");
                updateMyNeighborsFromRemote(strings[1]);
            }
            else if(strings[0].equals(Constants.DATA_FORWARD_REQUEST))
            {
                Log.v(TAG,"Data forward request handling");
                handleDataForwardRequest(strings[1]);
            }
            else if(strings[0].equals(Constants.DATA_QUERY_REQUEST))
            {
                Log.v(TAG,"Data query handling");
                handleDataQueryRequest(strings[1]);
            }
            else if(strings[0].equals(Constants.DATA_QUERY_REPLY))
            {
                Log.v(TAG,"Query reply handling");
                handleQueryReply(strings[1]);
            }
//            else if(strings[0].equals(Constants.ALL_DATA_QUERY_REQUEST))
//            {
//                Log.v(TAG,"All data Query handling");
//                handleAllDataQueryRequest(strings[1]);
//            }
//            else if(strings[0].equals(Constants.ALL_DATA_QUERY_REPLY))
//            {
//                Log.v(TAG,"All data Query reply handling");
//                handleAllDataQueryReply(strings[1]);
//            }

            return;
        }

        protected String handleNodeJoinRequest(String object) {
            try {

                Log.d(TAG, "Received join request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String remotePortStr = jsonObject.getString(Constants.MY_PORT);

                int remotePort = Integer.parseInt(remotePortStr); // remote port
                int remoteEmulatorId = remotePort/2; // emulator_id from port
                String remoteEmulatorIdStr = String.valueOf(remoteEmulatorId);

                remoteNode = new Node(genHash(remoteEmulatorIdStr), remotePortStr);
                ringList.add(remoteNode);

                Collections.sort(ringList);
                ArrayList<Node> list = new ArrayList<Node>(ringList);
                int index=0;

                for(int i=0;i<list.size();i++)
                {
                    Log.d(TAG, "Updating 5554 with neighbor info locally");

                    String nodeId = list.get(i).getNodeId();

                    if(nodeId.equals(my_node.getNodeId())) // This is to update pre and suc of 5554 node
                        updateMyNeighborsLocally(i, list);
                }

                for(int i=0;i<list.size();i++)
                {
                    Log.d(TAG, "List port:"+list.get(i).getPortNo()+" index:"+i);

                    String nodeId = list.get(i).getNodeId();

                    if(nodeId.equals(remoteNode.getNodeId()))
                       {
                           index=i;
                           break;
                       }
                }

                if(index == 0)
                {
                    // Predecessor is list.size()-1, Successor is index+1
                    predecessorNode = list.get(list.size()-1);
                    successorNode = list.get(index+1);
                }
                else if(index == list.size()-1)
                {
                    // Predecessor is index-1, successor is index 0
                    predecessorNode = list.get(index-1);
                    successorNode = list.get(0);
                }
                else
                {
                    // For any other index, predecessor is index-1 and successor is index+1
                    predecessorNode = list.get(index-1);
                    successorNode = list.get(index+1);
                }

                jsonObject.put(Constants.STATUS, 1); // 1 is reply to join request
                jsonObject.put(Constants.PREDECESSOR_PORT, predecessorNode.getPortNo());
                jsonObject.put(Constants.SUCCESSOR_PORT, successorNode.getPortNo());
                jsonObject.put(Constants.MY_PORT, my_node.getPortNo());
                return jsonObject.toString();

            } catch (JSONException e) {
                e.printStackTrace();
            }
            return "";
        }

        protected void updateMyNeighborsLocally(int i, ArrayList<Node> list)
        {
            if(i == 0)
            {
                // Predecessor is list.size()-1, Successor is index+1
                MY_PREDECESSOR_PORT = list.get(list.size()-1).getPortNo();
                MY_SUCCESSOR_PORT = list.get(i+1).getPortNo();
            }
            else if(i == list.size()-1)
            {
                // Predecessor is index-1, successor is index 0
                MY_PREDECESSOR_PORT = list.get(i-1).getPortNo();
                MY_SUCCESSOR_PORT = list.get(0).getPortNo();
            }
            else
            {
                // For any other index, predecessor is index-1 and successor is index+1
                MY_PREDECESSOR_PORT = list.get(i-1).getPortNo();
                MY_SUCCESSOR_PORT = list.get(i+1).getPortNo();
            }

            Log.v(TAG,"My port:"+my_node.getPortNo()+" MY Pre:"+MY_PREDECESSOR_PORT+" My Suc:"+MY_SUCCESSOR_PORT);

        }

        protected void updateMyNeighborsFromRemote(String object)
        {
            try {

                Log.d(TAG, "Update neighbor:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String newJoineePort = jsonObject.getString(Constants.MY_PORT);
                String newJoinee_p = jsonObject.getString(Constants.PREDECESSOR_PORT);
                String newJoinee_s = jsonObject.getString(Constants.SUCCESSOR_PORT);

                if(my_node.getPortNo().equals(newJoinee_p))
                {
                    // My successor will be newJoinee
                    MY_SUCCESSOR_PORT = newJoineePort;
                }
                else if(my_node.getPortNo().equals(newJoinee_s))
                {
                    // My predecessor will be newJoinee
                    MY_PREDECESSOR_PORT = newJoineePort;
                }

                Log.v(TAG,"My port:"+my_node.getPortNo()+" MY Pre:"+MY_PREDECESSOR_PORT+" My Suc:"+MY_SUCCESSOR_PORT);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void handleDataForwardRequest(String object)
        {
            try {

                Log.d(TAG, "Data forward request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String key = jsonObject.getString(Constants.KEY);
                String value = jsonObject.getString(Constants.VALUE);
                //int  count = jsonObject.getInt(Constants.COUNT);

                insertValues(key, value);

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void handleDataQueryRequest(String object)
        {
            try {

                Log.d(TAG, "Data query request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String key = jsonObject.getString(Constants.KEY);

                queryValues(key, object);

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void handleQueryReply(String object)
        {
            try {

                Log.d(TAG, "Data query reply:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String key = jsonObject.getString(Constants.KEY);
                String value = jsonObject.getString(Constants.VALUE);

                synchronized ( valueLock ) {
                    globalValue = value;
                    globalFlag = true;
                    valueLock.notifyAll();  // notifyAll() might be safer...
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }


        protected String handleAllDataQueryRequest(String object)
        {
            try {

                Log.d(TAG, "All Data query request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                //String queryOriginPort = jsonObject.getString(Constants.QUERY_ORIGIN_PORT);

                return queryAllValues();

            } catch (JSONException e) {
                e.printStackTrace();
            }

            return "";
        }

        protected void handleAllDataQueryReply(String object)
        {
            try {

                Log.d(TAG, "All Data query reply:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String content = jsonObject.getString(Constants.ALL_DATA_QUERY_CONTENT);

                String[] allFileContent = content.split(Constants.TEXT_SEPARATOR);

                for(String s: allFileContent)
                {
                    allDataQueryReplyList.add(s); // add each key value pair to the list
                }
                allDataQueryReplyCount++;

                if(allDataQueryReplyCount == 4) // Received content from all emulators
                {
                    synchronized ( valueLock ) {
                        globalFlag = true;
                        valueLock.notifyAll();  // notifyAll() might be safer...
                    }
                }


            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected String handleNodeListQueryRequest(String object)
        {
            try {

                Log.d(TAG, "Node list query request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                StringBuffer nodeListContent = new StringBuffer();

                for(int i=0;i<ringList.size();i++)
                {
                    nodeListContent.append(ringList.get(i).getPortNo());
                    if(i != ringList.size()-1)
                        nodeListContent.append(Constants.TEXT_SEPARATOR);
                }

                jsonObject.put(Constants.NODE_LIST_CONTENT, nodeListContent.toString());

                return jsonObject.toString();

            } catch (JSONException e) {
                e.printStackTrace();
            }

            return "";
        }
    }

    private void updateNeighbours(Node r, Node p, Node s)
    {
        String[] neighbourPorts = {p.getPortNo(),s.getPortNo()};
        for(String port : neighbourPorts)
        {
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port, constructNeighborUpdateObject(r, p, s), Constants.UPDATE_NEIGHBORS);
        }
    }

    private String constructNeighborUpdateObject(Node r, Node p, Node s)
    {
        // Details of the node which joined
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 2); // 2 for neighbor update
            jsonObject.put(Constants.PREDECESSOR_PORT, p.getPortNo()); // node id of predecessor
            jsonObject.put(Constants.SUCCESSOR_PORT, s.getPortNo()); // node id of successor
            jsonObject.put(Constants.MY_PORT, r.getPortNo()); // port of node which joined
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }
}
