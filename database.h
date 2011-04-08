#ifndef __database_h__
#define __database_h__

#include <mysql/mysql.h>
#include <mysql/my_global.h>
#include <string>
#include <vector>
#include <map>
#include "datastructures.h"

using std::string;
using std::vector;
using std::map;

extern int database_connect(const string& hostname, const string& database,
							const string& username, const string& password,
							unsigned int port);
extern int database_query_select(string query, MYSQL_RES*& res);
extern int database_query_update(string query);
extern void database_close();

const char QUERY_INSERT_CLIENT[] = 
    "INSERT INTO Client (numCPUs, numCores, hyperthreading, turboboost,"
                         "CPUName, cacheSize, cpuflags, memory, memoryFree,"
                         "cpuinfo, meminfo, message, wait, reAdaptInterval)"
    "VALUES (%i, %i, %i, %i, '%s', %i, '%s', %llu, %llu, '%s', '%s', '%s', %i, %i);";
extern int insert_client();

const char QUERY_DELETE_CLIENT[] = "DELETE FROM Client WHERE idClient=%i;";
extern int delete_client(int client_id);

const char QUERY_POSSIBLE_EXPERIMENTS[] = 
    "SELECT Experiment.idExperiment, Experiment.name, Experiment.priority "
    "FROM Experiment_has_gridQueue "
    "JOIN gridQueue ON gridQueue_idgridQueue=idgridQueue "
    "JOIN Experiment ON idExperiment=Experiment_idExperiment "
    "WHERE idgridQueue=%i AND Experiment.active=TRUE;";
extern int get_possible_experiments(int grid_queue_id, vector<Experiment>& experiments);

const char QUERY_EXPERIMENT_CPU_COUNT[] = 
    "SELECT Experiment_idExperiment, SUM(Experiment_has_Client.numCores) "
    "FROM Experiment_has_Client "
    "GROUP BY Experiment_idExperiment;";
extern int get_experiment_cpu_count(map<int, int>& cpu_count_by_experiment);

#endif
