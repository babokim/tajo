package nta.storage;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.conf.NtaConf;
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Tablet;
import nta.storage.exception.ReadOnlyException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RawFile2 {
	
	public static final Log LOG = LogFactory.getLog(RawFile2.class);
	
	private RawFile2() {
		
	}
	
	public static RawFileScanner getScanner(NtaConf conf, final Schema schema, final Tablet[] tablets) throws IOException {
		return new RawFileScanner(conf, schema, tablets);
	}
	
	public static RawFileAppender getAppender(NtaConf conf, final Path path, final Schema schema) throws IOException {
		return new RawFileAppender(conf, path, schema); 
	}

	private static final int SYNC_ESCAPE = -1;
	private static final int SYNC_HASH_SIZE = 16;
	private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE;
	public static int SYNC_INTERVAL;
	
	public static class RawFileScanner implements FileScanner {
		
		private NtaConf conf;
		private Schema schema;
		private FSDataInputStream in;
		private SortedSet<Tablet> tabletSet;
		private Iterator<Tablet> tableIter;
		private Tablet curTablet;
		private FileSystem fs;
		private byte[] sync;
		private byte[] checkSync;

		private long start, end;
		private long lastSyncPos;
		private long headerPos;
		
		public RawFileScanner(NtaConf conf, final Schema schema, final Tablet[] tablets) throws IOException {
			init(conf, schema, tablets);
		}
		
		public void init(NtaConf conf, final Schema schema, final Tablet[] tablets) throws IOException {
			this.conf = conf;
			this.schema = schema;
			this.tabletSet = new TreeSet<Tablet>();
			this.sync = new byte[SYNC_HASH_SIZE];
			this.checkSync = new byte[SYNC_HASH_SIZE];
			
			for (Tablet t: tablets) {
				this.tabletSet.add(t);
			}
			this.tableIter = tabletSet.iterator();
			openNextTablet();
		}
		
		private boolean openNextTablet() throws IOException {
			if (this.in != null) {
				this.in.close();
			}
			if (tableIter.hasNext()) {
				curTablet = tableIter.next();
				this.fs = curTablet.getFilePath().getFileSystem(this.conf);
				this.in = fs.open(curTablet.getFilePath());
				this.start = curTablet.getStartOffset();
				this.end = curTablet.getStartOffset() + curTablet.getLength();
				
				readHeader();
				headerPos = in.getPos();
				if (start < headerPos) {
					in.seek(headerPos);
				} else {
					in.seek(start);
				}
				if (in.getPos() != headerPos) {
					in.seek(in.getPos()-SYNC_SIZE);
					while(in.getPos() < end) {
						if (checkSync()) {
							lastSyncPos = in.getPos();
							break;
						} else {
							in.seek(in.getPos()+1);
						}
					}
				}
				return true;
			} else {
				return false;
			}
		}
		
		private void readHeader() throws IOException {
			SYNC_INTERVAL = in.readInt();
			in.read(this.sync, 0, SYNC_HASH_SIZE);
			lastSyncPos = in.getPos();
		}
		
		private boolean checkSync() throws IOException {
			in.readInt();							// escape
			in.read(checkSync, 0, SYNC_HASH_SIZE);	// sync
			if (!Arrays.equals(checkSync, sync)) {
				in.seek(in.getPos()-SYNC_SIZE);
				return false;
			} else {
				return true;
			}
		}

		@Override
		public Tuple next() throws IOException {
			boolean checkSyncFlag = true;
			if (in.available() == 0) {
				// Open next tablet
				if (!openNextTablet()) {
					return null;
				} else {
					checkSyncFlag = false;
				}
			}
			
			// check sync
			if (checkSyncFlag && checkSync()) {
				if (in.getPos() >= end) {
					if (!openNextTablet()) {
						return null;
					}
				}
				lastSyncPos = in.getPos();
			}
			
			if (in.available() == 0) {
				if (!openNextTablet()) {
					return null;
				}
			}
			
			int i;
			VTuple tuple = new VTuple(schema.getColumnNum());

			boolean [] contains = new boolean[schema.getColumnNum()];
			for (i = 0; i < schema.getColumnNum(); i++) {
				contains[i] = in.readBoolean();
			}

			Column col = null;
			for (i = 0; i < schema.getColumnNum(); i++) {
				if (contains[i]) {
					col = schema.getColumn(i);
					switch (col.getDataType()) {
					case BYTE:
						tuple.put(i, in.readByte());
						break;
					case SHORT:
						tuple.put(i, in.readShort());
						break;
					case INT:
						tuple.put(i, in.readInt());
						break;
					case LONG:
						tuple.put(i, in.readLong());
						break;
					case FLOAT:
						tuple.put(i, in.readFloat());
						break;
					case DOUBLE:
						tuple.put(i, in.readDouble());
						break;
					case STRING:
						short len = in.readShort();
						byte[] buf = new byte[len];
						in.read(buf, 0, len);
						tuple.put(i, new String(buf));
						break;
					case IPv4:
						byte[] ipv4 = new byte[4];
						in.read(ipv4, 0, 4);
						tuple.put(i, ipv4);
						break;
					default:
						break;
					}
				}
			}

			return tuple;
		}

		@Override
		public void reset() throws IOException {
			in.reset();
		}

		@Override
		public void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}
		
	}
	
	public static class RawFileAppender implements Appender {
		
		private NtaConf conf;
		private FSDataOutputStream out;
		private long lastSyncPos;
		private Path path;
		private Schema schema;
		private FileSystem fs;
		private byte[] sync;
		
		public RawFileAppender(NtaConf conf, final Path path, final Schema schema) throws IOException {
			this.conf = conf;
			SYNC_INTERVAL = conf.getInt(NConstants.RAWFILE_SYNC_INTERVAL, SYNC_SIZE*100);
			this.schema = schema;
			this.path = new Path(path, "data");
			fs = path.getFileSystem(conf);
			if (!fs.exists(path)) {
				fs.mkdirs(path);
			}
			sync = new byte[SYNC_HASH_SIZE];
			lastSyncPos = 0;
			init();
		}
		
		private void init() throws IOException {
			MessageDigest md;
			try {
				md = MessageDigest.getInstance("MD5");
				md.update((path.toString()+System.currentTimeMillis()).getBytes());
				sync = md.digest();
			} catch (NoSuchAlgorithmException e) {
				LOG.error(e);
			}
			Path tablePath = null;
			for (int i = 0; ; i++) {
				tablePath = new Path(path, "table"+i+".raw");
				if (!fs.exists(tablePath)) {
					out = fs.create(tablePath);
					break;
				}
			}
			writeHeader();
		}
		
		private void writeHeader() throws IOException {
			out.writeInt(SYNC_INTERVAL);
			out.write(sync);
			out.flush();
			lastSyncPos = out.getPos();
		}
		
		@Override
		public void addTuple(Tuple t) throws IOException {
			checkAndWriteSync();
			Column col = null;
			for (int i = 0; i < schema.getColumnNum(); i++) {
				out.writeBoolean(t.contains(i));
			}
			for (int i = 0; i < schema.getColumnNum(); i++) {
				if (t.contains(i)) {
					col = schema.getColumn(i);
					switch (col.getDataType()) {
					case BYTE:
						out.writeByte(t.getByte(i));
						break;
					case STRING:
						byte[] buf = t.getString(i).getBytes();
						if (buf.length > 256) {
							buf = new byte[256];
							byte[] str = t.getString(i).getBytes();
							System.arraycopy(str, 0, buf, 0, 256);
						} 
						out.writeShort(buf.length);
						out.write(buf, 0, buf.length);
						break;
					case SHORT:
						out.writeShort(t.getShort(i));
						break;
					case INT:
						out.writeInt(t.getInt(i));
						break;
					case LONG:
						out.writeLong(t.getLong(i));
						break;
					case FLOAT:
						out.writeFloat(t.getFloat(i));
						break;
					case DOUBLE:
						out.writeDouble(t.getDouble(i));
						break;
					case IPv4:
						out.write(t.getIPv4Bytes(i));
						break;
					case IPv6:
						out.write(t.getIPv6Bytes(i));
						break;
					default:
						break;
					}
				}
			}
		}

		@Override
		public void flush() throws IOException {
			out.flush();
		}

		@Override
		public void close() throws IOException {
			if (out != null) {
				sync();
				out.flush();
				out.close();
			}
		}
		
		private void sync() throws IOException {
			if (lastSyncPos != out.getPos()) {
				out.writeInt(SYNC_ESCAPE);
				out.write(sync);
				lastSyncPos = out.getPos();
			}
		}
		
		synchronized void checkAndWriteSync() throws IOException {
			if (out.getPos() >= lastSyncPos + SYNC_INTERVAL) {
				sync();
			}
		}
		
	}
}
