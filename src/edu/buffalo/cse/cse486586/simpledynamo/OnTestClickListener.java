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


public class OnTestClickListener implements OnClickListener {

	private final TextView mTextView;
	private final ContentResolver mContentResolver;
	private final Uri mUri;
	private final ContentValues[] mContentValues;
	private static final int TEST_CNT = 50;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";


	ContentValues cv=new ContentValues();
	Cursor resultCursor;
	int i;

	public OnTestClickListener(TextView _tv, ContentResolver _cr) {
		mTextView = _tv;
		mContentResolver = _cr;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		mContentValues = initTestValues();
	}

	private Uri buildUri(String scheme, String authority) {
		//URI builder
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private ContentValues[] initTestValues() {
		ContentValues[] cv = new ContentValues[TEST_CNT];
		for (int i = 0; i < TEST_CNT; i++) {
			cv[i] = new ContentValues();
			cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
			cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
		}

		return cv;
	}
	@Override
	public void onClick(View arg0) {
		// execute Async task on button click
		new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
	}
	private class Task extends AsyncTask<Void, String, Void> {

		@Override
		protected Void doInBackground(Void... params) {

			resultCursor =mContentResolver.query(mUri, null,"*", null, null);

			if (testInsert()) {
				publishProgress("Insert success\n");
			} else {
				publishProgress("Insert fail\n");
				return null;
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {

				e.printStackTrace();
			}

			if (testQuery()) {
				publishProgress("Query success\n");
			} else {
				publishProgress("Query fail\n");
			}

			return null;
		}

		protected void onProgressUpdate(String...strings) {
			mTextView.append(strings[0]);

			return;
		}

		private boolean testInsert() {
			try {
				for (int i = 0; i < TEST_CNT; i++) {
					mContentResolver.insert(mUri, mContentValues[i]);
				}
			} catch (Exception e) {
				Log.e("test button", e.toString());
				return false;
			}

			return true;
		}

		private boolean testQuery() {
			try {

				for (int i = 0; i <TEST_CNT; i++) 
				{

					String key = (String) mContentValues[i].get(KEY_FIELD);
					String val = (String) mContentValues[i].get(VALUE_FIELD);
					int k=mContentResolver.delete(mUri,key,null);

					// query the database for the input key
					Cursor resultCursor = mContentResolver.query(mUri, null, key, null, null);
					if (resultCursor == null) {
						Log.e("TAG", "Result null");
						throw new Exception();
					}
					// get the value from the result cursor for the input key field
					int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
					int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
					if (keyIndex == -1 || valueIndex == -1) {						//if result cursor is of size -1, then no result is found
						Log.e("TAG", "Wrong columns");
						resultCursor.close();
						throw new Exception();
					}

					resultCursor.moveToFirst();

					if (!(resultCursor.isFirst() && resultCursor.isLast())) {
						Log.e("TAG", "Wrong number of rows");
						resultCursor.close();
						throw new Exception();
					}

					String returnKey = resultCursor.getString(0);
					String returnValue = resultCursor.getString(1);
					if (!(returnKey.equals(key) && returnValue.equals(val))) {
						Log.e("TAG", "(key, value) pairs don't match\n");			// log the key-value pairs
						resultCursor.close();
						throw new Exception();
					}
				}
			} catch (Exception e) {
				return false;
			}

			return true;
		}
	}

}
