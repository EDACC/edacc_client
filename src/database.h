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

const int MIN_MODEL_VERSION = 22;
static const int DOWNLOAD_TIMEOUT = 10;
static const int DOWNLOAD_REFRESH = 2;

// how many times to try to reissue transactions when they failed because of deadlocks or timeouts (recoverable errors)
static const unsigned int max_recover_tries = 5;

bool is_recoverable_error();
extern int database_connect(const string& hostname, const string& database,
							const string& username, const string& password,
							unsigned int port);
int get_new_connection(MYSQL *&con);
string get_db();
string get_db_host();
string get_db_username();
string get_db_password();
int get_db_port();
int database_query_select(string query, MYSQL_RES*& res, MYSQL*& con);
extern int database_query_select(string query, MYSQL_RES*& res);
int database_query_update(string query, MYSQL *&con);
extern int database_query_update(string query);
extern void database_close();

const char QUERY_LOCK_CLIENT_FS[] =
    "SELECT GET_LOCK('CLIENT_FS_LOCK', 60)"; // wait max. 60 seconds to get the lock
const char QUERY_RELEASE_CLIENT_FS[] =
    "SELECT RELEASE_LOCK('CLIENT_FS_LOCK')";
const char QUERY_INSERT_CLIENT[] = 
    "INSERT INTO Client (numCores, numThreads, hyperthreading, turboboost,"
                         "CPUName, cacheSize, cpuflags, memory, memoryFree,"
                         "cpuinfo, meminfo, message, gridQueue_idgridQueue, "
                         "lastReport, jobs_wait_time, walltime)"
    "VALUES (%i, %i, %i, %i, '%s', %i, '%s', %llu, %llu, '%s', '%s', '%s', %i, NOW(), %i, %i);";
extern int insert_client(const HostInfo& host_info, int grid_queue_id, int jobs_wait_time, string& opt_walltime);

const char QUERY_FILL_GRID_QUEUE_INFO[] =
    "UPDATE gridQueue SET numCores=%i, numThreads=%i, hyperthreading=%i,"
    "turboboost=%i, CPUName='%s', cacheSize=%i, cpuflags='%s', memory=%llu,"
    "cpuinfo='%s', meminfo='%s' WHERE idgridQueue=%i AND numCores IS NULL;";
extern int fill_grid_queue_info(const HostInfo& host_info, int grid_queue_id);


const char QUERY_DELETE_CLIENT[] = "DELETE FROM Client WHERE idClient=%i;";
extern int delete_client(int client_id);


const char QUERY_POSSIBLE_EXPERIMENTS[] = 
    "SELECT Experiment.idExperiment, Experiment.name, Experiment.priority, Experiment.solverOutputPreserveFirst,"
    "Experiment.solverOutputPreserveLast, Experiment.watcherOutputPreserveFirst, Experiment.watcherOutputPreserveLast,"
    "Experiment.verifierOutputPreserveFirst, Experiment.verifierOutputPreserveLast, Experiment.Cost_idCost "
    "FROM Experiment "
    "JOIN Experiment_has_gridQueue ON Experiment_has_gridQueue.Experiment_idExperiment=Experiment.idExperiment "
    "WHERE gridQueue_idgridQueue=%d AND Experiment.active=TRUE AND Experiment.countUnprocessedJobs > 0 "
    "GROUP BY idExperiment";
// this is used for jobserver method; jobserver provides those ids.
const char QUERY_POSSIBLE_EXPERIMENTS_BY_EXPIDS[] =
	"SELECT Experiment.idExperiment, Experiment.name, Experiment.priority, Experiment.solverOutputPreserveFirst,"
	"Experiment.solverOutputPreserveLast, Experiment.watcherOutputPreserveFirst, Experiment.watcherOutputPreserveLast,"
	"Experiment.verifierOutputPreserveFirst, Experiment.verifierOutputPreserveLast, Experiment.Cost_idCost "
	"FROM Experiment WHERE idExperiment IN (%s);";
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
    "SELECT FLOOR(RAND()*countUnprocessedJobs) FROM Experiment "
    "WHERE idExperiment=%d;";
const char SELECT_ID_QUERY[] = 
    "SELECT idJob FROM ExperimentResults WHERE Experiment_idExperiment=%d "
    "AND status=-1 AND priority >= 0 LIMIT %d,1;";
const char SELECT_ID_QUERY_SB[] =
    "SELECT idJob FROM ExperimentResults AS er JOIN SolverConfig AS sc ON ("
    "er.SolverConfig_idSolverConfig = sc.idSolverConfig) WHERE er.Experiment_idExperiment=%d "
    "AND sc.idSolverConfig=%d AND status=-1 AND priority >= 0 LIMIT %d,1;";
const char SELECT_FOR_UPDATE[] = 
    "SELECT idJob, SolverConfig_idSolverConfig, Experiment_idExperiment, "
    "Instances_idInstance, run, seed, priority, CPUTimeLimit, wallClockTimeLimit, "
    "memoryLimit, stackSizeLimit "
    "FROM ExperimentResults WHERE idJob = %d and status=-1 FOR UPDATE;";
const char LOCK_JOB[] = 
    "UPDATE ExperimentResults SET status=0, startTime=NOW(), "
    "computeQueue=%d, computeNode='%s', computeNodeIP='%s', Client_idClient=%d "
    "WHERE idJob=%d;";
extern int db_fetch_job(int client_id, int grid_queue_id, int experiment_id, int solver_binary_id, Job& job);

const char QUERY_GRID_QUEUE_INFO[] =
    "SELECT name, location, numCPUs, numCPUsPerJob, description, numCores, CPUName "
    "FROM gridQueue WHERE idgridQueue=%i;";
extern int get_grid_queue_info(int grid_queue_id, GridQueue& grid_queue);

const char QUERY_SOLVER[] =
	"SELECT SolverBinaries.idSolverBinary, Solver.name, SolverBinaries.binaryName, SolverBinaries.md5, SolverBinaries.runCommand, SolverBinaries.runPath, Solver.idSolver "
	"FROM SolverBinaries LEFT JOIN SolverConfig ON (SolverBinaries.idSolverBinary = SolverConfig.SolverBinaries_idSolverBinary) "
    "LEFT JOIN Solver ON (Solver.idSolver = SolverBinaries.idSolver) "
	"WHERE idSolverConfig=%d;";

const char QUERY_INSTANCE[] =
	"SELECT name, md5 "
	"FROM Instances "
	"WHERE idInstance = %d;";
int get_solver(Job& job, Solver& solver);
int get_instance(Job& job, Instance& instance);

const char QUERY_CURRENT_MODEL_VERSION[] =
    "SELECT MAX(`version`) FROM Version";
int get_current_model_version();

const char QUERY_SOLVER_BINARY[] =
	"SELECT `binaryArchive` "
	"FROM SolverBinaries "
	"WHERE idSolverBinary = %d";

const char QUERY_INSTANCE_BINARY[] =
	"SELECT instance "
	"FROM Instances "
	"WHERE idInstance = %d";

const char QUERY_VERIFIER_BINARY[] =
	"SELECT `binaryArchive` "
	"FROM Verifier "
	"WHERE idVerifier = %d";

const char QUERY_COST_BINARY[] =
	"SELECT `binaryArchive` "
	"FROM CostBinary "
	"WHERE idCostBinary = %d";

const char QUERY_LOCK_FILE[] =
    "INSERT INTO LockedFiles (filename, filesystemID, lastReport) "
    "VALUES (\"%s\", %d, NOW());";

const char QUERY_UPDATE_FILE_LOCK[] =
    "UPDATE LockedFiles "
    "SET lastReport = NOW() "
    "WHERE filename = \"%s\" AND filesystemID = %d;";

const char QUERY_CHECK_FILE_LOCK[] =
    "SELECT TIMESTAMPDIFF(SECOND, lastReport, NOW()) "
    "FROM LockedFiles "
    "WHERE filename = \"%s\" AND filesystemID = %d FOR UPDATE;";

const char QUERY_UNLOCK_FILE[] =
    "DELETE FROM LockedFiles "
    "WHERE filename = \"%s\" AND filesystemID = %d;";

class Solver_lock_update {
public:
	Solver* solver;
	int fsid;
	int finished;
};

class Instance_lock_update {
public:
	Instance* instance;
	int fsid;
	int finished;
};

int get_instance_binary(Instance& instance, string& instance_binary);
int get_solver_binary(Solver& solver, string& solver_base_path);

const char QUERY_SOLVER_CONFIG_PARAMS[] =
    "SELECT idParameter, name, prefix, hasValue, defaultValue, `order`, space, attachToPrevious, value "
    "FROM Parameters JOIN SolverConfig_has_Parameters ON idParameter = Parameters_idParameter "
    "WHERE SolverConfig_idSolverConfig=%i ORDER BY `order`;";

extern int get_solver_config_params(int solver_config_id, vector<Parameter>& params);

const char QUERY_VERIFIER[] =
	"SELECT idVerifier, idVerifierConfig, name, md5, runCommand, runPath "
	"FROM VerifierConfig JOIN Verifier ON VerifierConfig.Verifier_idVerifier = Verifier.idVerifier "
	"WHERE VerifierConfig.Experiment_idExperiment = %d;";

const char QUERY_VERIFIER_PARAMETERS[] =
	"SELECT idVerifierParameter, name, prefix, hasValue, defaultValue, `order`, space, attachToPrevious, value "
	"FROM VerifierParameter JOIN VerifierConfig_has_VerifierParameter ON idVerifierParameter = VerifierParameter_idVerifierParameter "
	"WHERE VerifierConfig_idVerifierConfig=%i ORDER BY `order`;";

int get_verifier_binary(Verifier& verifier, string& verifier_base_path);
int get_verifier_details(Verifier& verifier, int idExperiment);

const char QUERY_COST_BINARY_DETAILS[] =
	"SELECT idCostBinary, Solver_idSolver, Cost_idCost, binaryName, md5, version, runCommand, runPath, parameters "
	"FROM CostBinary "
	"WHERE Solver_idSolver = %d AND Cost_idCost = %d;";

int get_cost_binary_details(CostBinary& cost_binary, int idSolver, int idCost);
int get_cost_binary(CostBinary& cost_binary, string& cost_binary_base_path);

const char QUERY_UPDATE_JOB[] = 
    "UPDATE ExperimentResults, ExperimentResultsOutput SET "
    "status=%d, resultCode=%d, resultTime=%f, wallTime=%f, solverOutput='%s', "
    "watcherOutput='%s', launcherOutput='%s', verifierOutput='%s', "
    "solverExitCode=%d, watcherExitCode=%d, verifierExitCode=%d, cost='%s' "
    "WHERE idJob=%d AND ExperimentResults_idJob=%d;";
extern int db_update_job(const Job& job);
    
const char QUERY_RESET_JOB[] = 
    "UPDATE ExperimentResults "
    "SET status=-1, startTime=NULL, computeQueue=NULL, "
    "computeNode=NULL, Client_idClient=NULL "
    "WHERE idJob=%d";
extern int db_reset_job(int job_id);

const char LOCK_MESSAGE[] =
    "SELECT message FROM Client WHERE idClient = %d FOR UPDATE;";
const char CLEAR_MESSAGE[] =
    "UPDATE Client SET message = '', lastReport=NOW(), jobs_wait_time = %i, current_wait_time = %i WHERE idClient = %d";
int get_message(int client_id, int jobs_wait_time, int current_wait_time, string& message, MYSQL* con);

bool db_fetch_jobs_for_simulation(int grid_queue_id, vector<Job*> &jobs);
const char QUERY_FETCH_JOBS_SIMULATION[] =
        "SELECT idJob, SolverConfig_idSolverConfig, Experiment_idExperiment, "
        "Instances_idInstance, run, seed, ExperimentResults.priority, CPUTimeLimit, wallClockTimeLimit, "
        "memoryLimit, stackSizeLimit, outputSizeLimitFirst, outputSizeLimitLast "
        "FROM ExperimentResults JOIN Experiment ON (idExperiment = Experiment_idExperiment) "
        "WHERE Experiment_idExperiment IN "
        "(SELECT Experiment_idExperiment FROM Experiment_has_gridQueue WHERE gridQueue_idgridQueue = %d) AND status = -1 AND run = 0 AND active = 1";

bool db_get_status_code_description(int status_code, string &description);
const char QUERY_STATUS_CODE_DESCRIPTION[] =
        "SELECT description FROM StatusCodes WHERE statusCode = %d";
#endif
