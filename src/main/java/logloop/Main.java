package logloop;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.sql.DataSource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.json.JSONException;
import org.json.JSONObject;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.HashPrefixStatementRewriter;
import org.slf4j.LoggerFactory;

/**
 * Opens a socket and writes each received line into the database. Events are
 * stored with UUID and a timestamp. Flat JSON data is split and its fields are
 * queryable separately.
 */
public class Main {
	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new Error("Could not load PostgreSQL driver.", e);
		}
	}
	private static DataSource dataSource;
	private static DBI dbi;

	/**
	 * Holds log events that have been read from the network but are not yet
	 * persisted.
	 */
	private static Queue<Event> events = new ConcurrentLinkedQueue<>();
	/**
	 * Pool of workers that consume the {@link #events} and persist them.
	 */
	private static ScheduledExecutorService indexers;

	enum CommandLineOptions {
		DATABASE("JDBC connection string."), DATABASE_POOL_SIZE(
				"Connection pool size."), PORT(
				"Port on which to accept events to forward to database.");
		private String description;

		private CommandLineOptions(String description) {
			this.description = description;
		}

		String getOptionName() {
			return name().toLowerCase();
		}
	}

	public static void main(String[] args) throws ClassNotFoundException,
			ParseException {
		CommandLine cmd = parse(args);
		String url = cmd.getOptionValue(CommandLineOptions.DATABASE
				.getOptionName());
		int poolSize = Integer.valueOf(cmd
				.getOptionValue(CommandLineOptions.DATABASE_POOL_SIZE
						.getOptionName()));
		int port = Integer.valueOf(cmd.getOptionValue(CommandLineOptions.PORT
				.getOptionName()));

		dataSource = setupDataSource(url, poolSize);
		indexers = Executors.newScheduledThreadPool(poolSize);
		for (int writerIndex = 0; writerIndex < poolSize; writerIndex++) {
			WriteTask writer = new WriteTask(events, getDBI(), indexers);
			indexers.submit(writer);
		}

		// 1MB buffer (max size of incoming messages).
		ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
		// Attempt to open socket to listen for log events. Try to reopen in
		// case of any failure.
		while (true) {
			try (Selector selector = Selector.open();
					ServerSocketChannel server1 = ServerSocketChannel.open();) {
				server1.configureBlocking(false);
				server1.socket().bind(new InetSocketAddress(port));
				server1.register(selector, SelectionKey.OP_ACCEPT);
				// Continuously await data on socket.
				while (true) {
					selector.select();
					Iterator<SelectionKey> iter = selector.selectedKeys()
							.iterator();
					while (iter.hasNext()) {
						SocketChannel client;
						SelectionKey key = iter.next();
						iter.remove();
						switch (key.readyOps()) {
						case SelectionKey.OP_ACCEPT:
							client = ((ServerSocketChannel) key.channel())
									.accept();
							client.configureBlocking(false);
							client.register(selector, SelectionKey.OP_READ);
							break;
						// Take each received line and save it as log event.
						case SelectionKey.OP_READ:
							client = (SocketChannel) key.channel();
							buffer.clear();
							if (client.read(buffer) != -1) {
								buffer.flip();
								BufferedReader linesReader = new BufferedReader(
										new InputStreamReader(
												new ByteArrayInputStream(
														buffer.array(),
														buffer.position(),
														buffer.remaining())));
								String line;
								while ((line = linesReader.readLine()) != null) {
									// Check if line is JSON. If so extract
									// data, otherwise generate identifier.
									UUID uuid = null;
									Timestamp timestamp = null;
									String message = null;
									Map<String, String> fields = new HashMap<>();
									try {
										JSONObject json = new JSONObject(line);
										@SuppressWarnings("unchecked")
										Set<String> jsonKeys = (Set<String>) json
												.keySet();
										for (String eventKey : jsonKeys) {
											String value = json.get(eventKey)
													.toString();
											if (eventKey.equals("uuid")) {
												try {
													uuid = UUID
															.fromString(value);
												} catch (IllegalArgumentException e) {
													// UUID did not confirm to
													// RFC 4122.
													fields.put(eventKey, value);
												}
											} else if (eventKey
													.equals("@timestamp")) {
												try {
													Calendar time = javax.xml.bind.DatatypeConverter
															.parseDateTime(value);
													timestamp = new Timestamp(
															time.getTimeInMillis());
												} catch (IllegalArgumentException e) {
													// Date did not confirm to
													// ISO 8601.
													fields.put(eventKey, value);
												}
											} else if (eventKey
													.equals("message")) {
												message = value;
											} else {
												fields.put(eventKey, value);
											}
										}
									} catch (JSONException e) {
										// Fallback if line did not contain
										// valid JSON.
										message = line;
									}
									if (uuid == null) {
										// Generate identifier as application
										// failed to do so.
										uuid = UUID.randomUUID();
									}
									if (timestamp == null) {
										timestamp = new Timestamp(Calendar
												.getInstance()
												.getTimeInMillis());
									}
									// Queue as log event.
									Event event = new Event(uuid, timestamp,
											message, fields);
									events.add(event);
								}
							} else {
								key.cancel();
							}
							break;
						default:
							LoggerFactory.getLogger(Main.class).warn(
									"unhandled NIO key: " + key.readyOps());
							break;
						}
					}
				}

			} catch (IOException e2) {
				LoggerFactory.getLogger(Main.class).error(
						"IO error, will try to re-listen.", e2);
			}
		}
	}

	/**
	 * Parses command line options.
	 */
	private static CommandLine parse(String[] args) throws ParseException {
		Options options = new Options();

		for (CommandLineOptions option : CommandLineOptions.values()) {
			Option cmdOption = new Option(option.getOptionName(), true,
					option.description);
			cmdOption.setRequired(true);
			options.addOption(cmdOption);
		}

		CommandLineParser parser = new GnuParser();
		try {
			return parser.parse(options, args, true);
		} catch (ParseException e) {
			new HelpFormatter().printHelp("java -jar logloop.jar", options,
					true);
			throw e;
		}
	}

	private static DataSource setupDataSource(String connectURI, int poolSize) {
		//
		// First, we'll create a ConnectionFactory that the
		// pool will use to create Connections.
		// We'll use the DriverManagerConnectionFactory,
		// using the connect string passed in the command line
		// arguments.
		//
		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
				connectURI, null);
		//
		// Next we'll create the PoolableConnectionFactory, which wraps
		// the "real" Connections created by the ConnectionFactory with
		// the classes that implement the pooling functionality.
		//
		GenericObjectPool dbPool = new GenericObjectPool();
		dbPool.setMaxIdle(poolSize);
		dbPool.setMaxActive(poolSize);
		new PoolableConnectionFactory(connectionFactory, dbPool, null,
				"select 1", false, true);

		//
		// Finally, we create the PoolingDriver itself,
		// passing in the object pool we created.
		//
		PoolingDataSource dataSource = new PoolingDataSource(dbPool);
		return dataSource;
	}

	public static DBI getDBI() {
		if (dbi == null) {
			synchronized (dataSource) {
				if (dbi == null) {
					DataSource ds = dataSource;
					dbi = new DBI(ds);
					dbi.setStatementRewriter(new HashPrefixStatementRewriter());
				}
			}
		}
		return dbi;
	}
}
