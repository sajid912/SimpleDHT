package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.TextView;
import android.widget.Toast;

// Edited
public class SimpleDhtActivity extends Activity {

    private String myPort;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        initializeControls();
    }

    private void initializeControls()
    {
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Toast.makeText(getApplicationContext(), "My port number:" + myPort, Toast.LENGTH_SHORT).show();

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver())); // Test button
        findViewById(R.id.button1).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver())); // LDump button
        findViewById(R.id.button2).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver())); // GDump button
    }
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
