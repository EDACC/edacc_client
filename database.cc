#include <string>
#include <mysql/mysql.h>
#include <mysql/my_global.h>
#include <mysql/errmsg.h>

#include "database.h"
#include "log.h"

using std::string;

int database_connect() {
    if (connection != 0) mysql_close(connection);
    
    connection = mysql_init(NULL);
    if (connection == NULL) {
        return 0;
    }
    
    if (mysql_real_connect(connection, hostname.c_str(), user.c_str(), 
                           password.c_str(), database.c_str(), port,
                           NULL, 0) == NULL) {
        log_error(AT, "Database connection attempt failed.");
        return 0;
    }
    
    return 1;
}

int database_query(string query, MYSQL_RES* res) {
    int status = mysql_query(connection, query.c_str());
    if (status != 0) {
        log_error(AT, "Query failed: %s", query.c_str());
        return 0; 
    }
    
    if ((res = mysql_store_result(connection)) == NULL) {
        log_error(AT, "Couldn't fetch query result of query: %s", query.c_str());
        return 0;
    }
    
    return 1;
}
