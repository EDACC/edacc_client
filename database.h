#ifndef __database_h__
#define __database_h__

#include <mysql/mysql.h>
#include <mysql/my_global.h>
#include <string>
using std::string;

static string hostname, database, user, password;
unsigned int port;

MYSQL* connection = 0;

extern int database_connect();
extern int database_query(string query, MYSQL_RES* res);

#endif
