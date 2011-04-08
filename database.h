#ifndef __database_h__
#define __database_h__

#include <mysql/mysql.h>
#include <mysql/my_global.h>
#include <string>
using std::string;

extern int database_connect(const string& hostname, const string& database,
							const string& username, const string& password,
							unsigned int port);
extern int database_query(string query, MYSQL_RES* res);
extern void database_close();

#endif
