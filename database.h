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


const char QUERY_INSERT_CLIENT[] = 
    "INSERT INTO Client (numCPUs, numCores, hyperthreading, turboboost,"
                         "CPUName, cacheSize, cpuflags, memory, memoryFree,"
                         "cpuinfo, meminfo, message, wait, reAdaptInterval)"
    "VALUES (%i, %i, %i, %i, '%s', %i, '%s', %i, %i, '%s', '%s', '%s', %i, %i);";

#endif
