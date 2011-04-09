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


const char QUERY_UPDATE_CORE_COUNT[] =
    "INSERT INTO Experiment_has_Client (Experiment_idExperiment, Client_idClient, "
                                        "numCores) "
    "VALUES (%i, %i, 1) ON DUPLICATE KEY UPDATE numCores=numCores+1";
extern int increment_core_count(int client_id, int experiment_id);

const char QUERY_DECREMENT_CORE_COUNT[] =
    "UPDATE Experiment_has_Client SET numCores=numCores-1 "
    "WHERE Experiment_idExperiment=%i AND Client_idClient=%i;";
extern int decrement_core_count(int client_id, int experiment_id);

const char LIMIT_QUERY[] = 
    "SELECT FLOOR(RAND()*COUNT(idJob)) as x FROM ExperimentResults \
    WHERE Experiment_idExperiment=%d AND status < 0 AND priority > 0;";
const char SELECT_ID_QUERY[] = 
    "SELECT idJob FROM ExperimentResults WHERE Experiment_idExperiment=%d \
                             AND status < 0 AND priority > 0 LIMIT %d,1;";
const char SELECT_FOR_UPDATE[] = 
    "SELECT idJob, SolverConfig_idSolverConfig, Experiment_idExperiment, "
    "Instances_idInstance, run, seed, priority, CPUTimeLimit, wallClockTimeLimit, "
    "memoryLimit, stackSizeLimit, outputSizeLimit "
    "FROM ExperimentResults WHERE idJob = %d and status<0 FOR UPDATE;";
const char LOCK_JOB[] = 
    "UPDATE ExperimentResults SET status=0, startTime=NOW(), "
    "computeQueue=%d, computeNode='%s', computeNodeIP='%s' "
    "WHERE idJob=%d;";
extern int db_fetch_job(int grid_queue_id, int experiment_id, Job& job);

#endif
