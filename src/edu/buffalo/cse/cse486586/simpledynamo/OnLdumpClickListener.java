package edu.buffalo.cse.cse486586.simpledynamo;


import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnLdumpClickListener implements OnClickListener {

	private static final String TAG = "Ldump";
	private final TextView mTextView;
	private final ContentResolver mContentResolver;
	private final Uri mUri;
	private final ContentValues[] mContentValues;

	private static final int TEST_CNT = 50;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	public OnLdumpClickListener(TextView _tv, ContentResolver _cr) {

		mTextView = _tv;
		mContentResolver = _cr;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		mContentValues = initTestValues();
	}

	private ContentValues[] initTestValues() {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		//store the key-value pairs in content value
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
			cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
			cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
		}

		return cv;
	}

	private Uri buildUri(String scheme, String authority) {
		//URI builder
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public void onClick(View arg0) {
		//execute AsyncTask on button click
		new LdumpTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
	}

	private class LdumpTask extends AsyncTask<Void, String, Void> {

		@Override
		protected Void doInBackground(Void... params) {

			if (testQuery()) {
				publishProgress("ldump done\n");
			} else {
				publishProgress("ldump fail\n");
			}

			return null;
		}

		protected void onProgressUpdate(String...strings) {
			mTextView.append(strings[0]);		//print the text on the screen
			return;
		}

		private boolean testQuery() {
			try {
				//test if correct value are retrieved for the query
				String key="@";
				Cursor resultCursor = mContentResolver.query(mUri, null,key, null, null);
				if (resultCursor == null) {
					Log.e(TAG, "Result null");
					throw new Exception();
				}

				int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
				int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
				if (keyIndex == -1 || valueIndex == -1) {				//if result cursor is of size -1, then no result is found
					Log.e(TAG, "Wrong columns");
					resultCursor.close();
					throw new Exception();
				}
				//move the cursor back to first position
				resultCursor.moveToFirst();

				while(resultCursor.isAfterLast() == false){
					//display the key-value pairs on the screen
					String returnKey = resultCursor.getString(0);
					String returnValue = resultCursor.getString(1);
					publishProgress(returnKey+" : "+ returnValue +"\n");
					resultCursor.moveToNext();

				}
				resultCursor.close();

			} catch (Exception e) {
				return false;
			}

			return true;
		}
	}
}

