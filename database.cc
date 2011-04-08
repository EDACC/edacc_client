#include <string>
#include <mysql/mysql.h>
#include <mysql/my_global.h>
#include <mysql/errmsg.h>

#include "database.h"
#include "log.h"

using std::string;

static MYSQL* connection = 0;
		
/**
 * Establishes a database connection with the specified connection details.
 * 
 * Returns 0 on errors, 1 on success.
 */
int database_connect(const string& hostname, const string& database,
							const string& username, const string& password,
							unsigned int port) {
    if (connection != 0) mysql_close(connection);
    
    connection = mysql_init(NULL);
    if (connection == NULL) {
        return 0;
    }
    
    if (mysql_real_connect(connection, hostname.c_str(), username.c_str(), 
                           password.c_str(), database.c_str(), port,
                           NULL, 0) == NULL) {
        log_error(AT, "Database connection attempt failed.");
        return 0;
    }
    
    // enable auto-reconnect on SERVER_LOST and SERVER_GONE errors
    // e.g. due to connection time outs. Failed queries have to be
    // re-issued in any case.
    int mysql_opt_reconnect = 1;
	mysql_options(connection, MYSQL_OPT_RECONNECT, &mysql_opt_reconnect);
    
    log_message(0, "Established database connection to %s:%s@%s:%u/%s",
					username.c_str(), password.c_str(), hostname.c_str(),
					port, database.c_str());
    return 1;
}

/**
 * Issues a query. If the database connection timed out this function will
 * attempt to re-issue the query once.
 * 
 * The query may not contain any null bytes.
 * 
 * Returns 0 on errors, 1 on success.
 */
int database_query(string query, MYSQL_RES* res) {
    int status = mysql_query(connection, query.c_str());
    if (status != 0) {
		if (status == CR_SERVER_GONE_ERROR || status == CR_SERVER_LOST) {
			// server connection lost, try to re-issue query once
			if (mysql_query(connection, query.c_str()) != 0) {
				// still doesn't work
				log_error(AT, "Lost connection to server and couldn't \
							reconnect when executing query: %s", query.c_str());
				return 0;
			}
			else {
				// successfully re-issued query
				log_message(0, "Lost connection but successfully re-established \
								when executing query: %s", query.c_str());
				return 1;
			}
		}
		
        log_error(AT, "Query failed: %s", query.c_str());
        return 0; 
    }
    
    if ((res = mysql_store_result(connection)) == NULL) {
        log_error(AT, "Couldn't fetch query result of query: %s", query.c_str());
        return 0;
    }
    
    return 1;
}

/**
 * Closes the database connection.
 */
void database_close() {
	mysql_close(connection);
	log_message(0, "Closed database connection");
}
