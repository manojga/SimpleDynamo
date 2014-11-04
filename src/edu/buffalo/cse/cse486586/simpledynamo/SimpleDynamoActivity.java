package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        //button1 to test the key-value pairs
        findViewById(R.id.button1).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        
        //button2 for displaying the local dump of key-values
        findViewById(R.id.button2).setOnClickListener(
                new OnLdumpClickListener(tv, getContentResolver()));
        
        //button3 for printing the key-value dump from all nodes
        findViewById(R.id.button3).setOnClickListener(
                new OnGdumpClickListener(tv, getContentResolver()));
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}

}
