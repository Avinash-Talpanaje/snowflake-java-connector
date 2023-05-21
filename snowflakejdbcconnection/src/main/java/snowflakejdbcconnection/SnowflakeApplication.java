
package snowflakejdbcconnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.jdbc.SnowflakeConnection;
import net.snowflake.client.jdbc.SnowflakeResultSet;
import net.snowflake.client.jdbc.SnowflakeStatement;

public class SnowflakeApplication {
	public static void main(String[] args) throws SQLException {
		String user = System.getenv("SNOWFLAKE_USER");
		String password = System.getenv("SNOWFLAKE_PASSWORD");
		String account = System.getenv("SNOWFLAKE_ACCOUNT");
		String role = System.getenv("SNOWFLAKE_USER_ROLE");

		Properties props = new Properties();
		props.put("authenticator", "snowflake");
		props.put("user", user);
		props.put("password", password);
		props.put("role", role == null || role == "" ? "USERADMIN" : role);

		props.put("schema", "TPCH_SF1");
		props.put("warehouse", "COMPUTE_WH");
		props.put("db", "SNOWFLAKE_SAMPLE_DATA");

		String url = "jdbc:snowflake://" + account + ".snowflakecomputing.com/";

		Connection connection = null;
		try {
			connection = DriverManager.getConnection(url, props);
			if (connection != null) {
				String sql_command = "";
				ResultSet resultSet;
				String queryID = "";

				System.out.println("Create JDBC statement.");
				Statement statement = connection.createStatement();
				sql_command = "SELECT * FROM CUSTOMER LIMIT 25";
				System.out.println("Simple SELECT query: " + sql_command);

				resultSet = statement.executeQuery(sql_command);
				resultSet.next();
				System.out.println(resultSet.getDouble("C_ACCTBAL"));

				System.out.println("Create JDBC statement.");
				statement = connection.createStatement();
				sql_command = "SELECT * FROM CUSTOMER LIMIT 25";
				System.out.println("Simple SELECT query: " + sql_command);
				resultSet = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(sql_command);
				queryID = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
				System.out.println("INFO: Closing statement.");
				statement.close();
				System.out.println("INFO: Closing connection.");
				connection.close();

				System.out.println("INFO: Re-opening connection.");
				connection = DriverManager.getConnection(url, props);
				resultSet = connection.unwrap(SnowflakeConnection.class).createResultSet(queryID);

				// Assume that the query isn't done yet.
				QueryStatus queryStatus = QueryStatus.RUNNING;
				while (queryStatus == QueryStatus.RUNNING) {
					Thread.sleep(2000); // 2000 milliseconds.
					queryStatus = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
				}

				if (queryStatus == QueryStatus.FAILED_WITH_ERROR) {
					System.out.format("ERROR %d: %s%n", queryStatus.getErrorMessage(), queryStatus.getErrorCode());
				} else if (queryStatus != QueryStatus.SUCCESS) {
					System.out.println("ERROR: unexpected QueryStatus: " + queryStatus);
				} else {
					while (resultSet.next()) {
						double account_balance = resultSet.getDouble("C_ACCTBAL");
						System.out.println("balance = " + account_balance);
					}
				}

			}
		} catch (SQLException | InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (!connection.isClosed()) {
				System.out.println("Closing the connection.");

			}
		}
	}
}
