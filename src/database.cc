#include <string>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <map>
#include <mysql/mysql.h>
#include <mysql/my_global.h>
#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>
#include <pthread.h>

#include "host_info.h"
#include "database.h"
#include "log.h"
#include "file_routines.h"
#include "lzma.h"

using std::string;
using std::vector;
using std::map;

extern string base_path;
extern string solver_path;
extern string instance_path;

static MYSQL* connection = 0;

// from client.cc
extern time_t opt_wait_jobs_time; // seconds

static time_t WAIT_BETWEEN_RECONNECTS = 5;
		
/**
 * Establishes a database connection with the specified connection details.
 * 
 * @param hostname DB host IP/DNS
 * @param database DB name
 * @param username DB username
 * @param password DB password
 * @param port DB port
 * @return 0 on errors, 1 on success.
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
        log_error(AT, "Database connection attempt failed: %s", mysql_error(connection));
        return 0;
    }
    
    // enable auto-reconnect on SERVER_LOST and SERVER_GONE errors
    // e.g. due to connection time outs. Failed queries have to be
    // re-issued in any case.
    my_bool mysql_opt_reconnect = 1;
    mysql_options(connection, MYSQL_OPT_RECONNECT, &mysql_opt_reconnect);
    
    log_message(LOG_INFO, "Established database connection to %s:%s@%s:%u/%s",
					username.c_str(), password.c_str(), hostname.c_str(),
					port, database.c_str());
    return 1;
}

int get_new_connection(MYSQL *&con) {
    if (con == NULL) mysql_close(con);
    con = mysql_init(NULL);
    if (con == NULL || connection == NULL) {
        return 0;
    }
    if (mysql_real_connect(con, connection->host, connection->user, connection->passwd, connection->db, connection->port, NULL, 0) == NULL) {
        log_error(AT, "Database connection attempt failed: %s", mysql_error(con));
        return 0;
    }
    return 1;
}

int database_query_select(string query, MYSQL_RES*& res, MYSQL*& con) {
    int status = mysql_query(con, query.c_str());
    if (status != 0) {
        if (mysql_errno(con) == CR_SERVER_GONE_ERROR || mysql_errno(con) == CR_SERVER_LOST) {
            // server connection lost, try to re-issue query once
            for (int i = 0; i < opt_wait_jobs_time / WAIT_BETWEEN_RECONNECTS; i++) {
                sleep(WAIT_BETWEEN_RECONNECTS);
                if (mysql_query(con, query.c_str()) != 0) {
                    // still doesn't work
                    log_error(AT, "Lost connection to server and couldn't \
                            reconnect when executing query: %s - %s", query.c_str(), mysql_error(con));
                } else {
                    // successfully re-issued query
                    log_message(LOG_INFO, "Lost connection but successfully re-established \
                                when executing query: %s", query.c_str());
                    return 1;
                }

            }
            return 0;
        }

        log_error(AT, "Query failed: %s, return code (status): %d errno: %d", query.c_str(), status, mysql_errno(con));
        return 0; 
    }
    
    if ((res = mysql_store_result(con)) == NULL) {
        log_error(AT, "Couldn't fetch query result of query: %s - %s",
                    query.c_str(), mysql_error(con));
        return 0;
    }
    
    return 1;
}

int database_query_update(string query, MYSQL *&con) {
    int status = mysql_query(con, query.c_str());
    if (status != 0) {
        if (mysql_errno(con) == CR_SERVER_GONE_ERROR || mysql_errno(con) == CR_SERVER_LOST) {
            // server connection lost, try to re-issue query once
            for (int i = 0; i < opt_wait_jobs_time / WAIT_BETWEEN_RECONNECTS; i++) {
                sleep(WAIT_BETWEEN_RECONNECTS);
                if (mysql_query(con, query.c_str()) != 0) {
                    // still doesn't work
                    log_error(AT, "Lost connection to server and couldn't \
                            reconnect when executing query: %s - %s", query.c_str(), mysql_error(con));
                } else {
                    // successfully re-issued query
                    log_message(LOG_INFO, "Lost connection but successfully re-established \
                                when executing query: %s", query.c_str());
                    return 1;
                }
                return 0;
            }
        }

        log_error(AT, "Query failed: %s, return code (status): %d errno: %d", query.c_str(), status, mysql_errno(con));
        return 0;
    }

    return 1;
}

/**
 * Issues a query. If the database connection timed out this function will
 * attempt to re-issue the query once.
 *
 * The query may not contain any null bytes.
 *
 * @param query The query that should be executed
 * @param res reference to a mysql result datastructure where the query results will be stored
 * @return 0 on errors, 1 on success.
 */
int database_query_select(string query, MYSQL_RES*& res) {
    return database_query_select(query, res, connection);
}

/**
 * Issues an insert/update query. If the database connection timed out this function will
 * attempt to re-issue the query once.
 * 
 * The query may not contain any null bytes.
 * 
 * @param query query that should be executed
 * @return 0 on errors, 1 on success.
 */
int database_query_update(string query) {
    return database_query_update(query, connection);
}

/**
 * Closes the database connection.
 */
void database_close() {
	mysql_close(connection);
	log_message(LOG_INFO, "Closed database connection");
}

/**
 * Executes the query needed to insert a new row into the Client table
 * and returns the auto-incremented ID of it.
 * 
 * @return id > 0 on success, 0 on errors
 */ 
int insert_client() {
    int num_cores = get_num_physical_cpus();
    int num_cpus = get_num_processors();
    bool hyperthreading = has_hyperthreading();
    bool turboboost = has_turboboost();
    string cpu_model = get_cpu_model();
    int cache_size = get_cache_size();
    string cpu_flags = get_cpu_flags();
    unsigned long long int memory = get_system_memory();
    unsigned long long int free_memory = get_free_system_memory();
    string cpuinfo = get_cpuinfo();
    string meminfo = get_meminfo();
    
    char* query = new char[32768];
    snprintf(query, 32768, QUERY_INSERT_CLIENT, num_cpus, num_cores,
                hyperthreading, turboboost, cpu_model.c_str(), cache_size,
                cpu_flags.c_str(), memory, free_memory, cpuinfo.c_str(),
                meminfo.c_str(), "", 0, 0);
    if (database_query_update(query) == 0) {
        log_error(AT, "Error when inserting into client table: %s", mysql_error(connection));
        delete[] query;
        return 0;
    }
    delete[] query;
    
    int id = mysql_insert_id(connection);
    return id;
}

/**
 * Executes the query needed to delete the client row with the given id from the Client table.
 * @param client_id id of the row that should be deleted
 * @return 1 on success, 0 on errors
 */
int delete_client(int client_id) {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_DELETE_CLIENT, client_id);
    if (database_query_update(query) == 0) {
        log_error(AT, "Error when deleting client from table: %s", mysql_error(connection));
        delete[] query;
        return 0;
    }
    delete[] query;
    return 1;
}

/**
 * Queries for active experiments with unprocessed jobs that are linked to the given grid queue.
 * 
 * @param grid_queue_id id of the client's assigned grid queue
 * @param experiments (empty) vector reference that will be filled with Experiment instances of the query results.
 * @return 1 on success, 0 on errors
 */
int get_possible_experiments(int grid_queue_id, vector<Experiment>& experiments) {
    char* query = new char[4096];
    snprintf(query, 4096, QUERY_POSSIBLE_EXPERIMENTS, grid_queue_id);
    MYSQL_RES* result = 0;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Error querying for list of experiments: %s", mysql_error(connection));
        delete[] query;
        mysql_free_result(result);
        return 0;
    }
    delete[] query;
    MYSQL_ROW row;

    int num_experiments = mysql_num_rows(result);
    while ((row = mysql_fetch_row(result))) {
        experiments.push_back(Experiment(atoi(row[0]), row[1], atoi(row[2])));
    }
    mysql_free_result(result);
    return num_experiments;
}

/**
 * Queries for the current number of CPUs processing each experiment as stored in the
 * Experiment_has_Client table.
 * The results are assigned to the passed in map<experiment_id, cpu count>.
 * 
 * @param cpu_count_by_experiment key-value map that should be filled with the results
 * @return 1 on success, 0 on errors
 */
int get_experiment_cpu_count(map<int, int>& cpu_count_by_experiment) {
    char* query = new char[4096];
    snprintf(query, 4096, QUERY_EXPERIMENT_CPU_COUNT);
    MYSQL_RES* result = 0;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Error querying for list of experiment cpu count: %s", mysql_error(connection));
        delete[] query;
        mysql_free_result(result);
        return 0;
    }
    delete[] query;
    MYSQL_ROW row;
    int num_experiments = mysql_num_rows(result);
    while ((row = mysql_fetch_row(result))) {
        cpu_count_by_experiment[atoi(row[0])] = atoi(row[1]);
    }
    mysql_free_result(result);
    return num_experiments;
}

/**
 * Executes an update query to increase the number of CPUs the client controls working on the experiment
 * by 1. This is an INSERT .. ON DUPLICATE KEY UPDATE .. query so it also works
 * if there is no Experiment_has_Client row for the client and experiment yet.
 * 
 * @param client_id the ID of the client
 * @param experiment_id the ID of the experiment
 * @return 1 on success, 0 on errors
 */
int increment_core_count(int client_id, int experiment_id) {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_UPDATE_CORE_COUNT, experiment_id, client_id);
    if (database_query_update(query) == 0) {
        log_error(AT, "Error when updating numCores: %s", mysql_error(connection));
        delete[] query;
        return 0;
    }
    delete[] query;
    return 1;
}

/**
 * Executes a query to decrease the number of CPUs the client controls working on the experiment
 * by 1.
 * 
 * @param client_id the ID of the client
 * @param experiment_id the ID of the experiment
 * @return 1 on success, 0 on errors
 */
int decrement_core_count(int client_id, int experiment_id) {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_DECREMENT_CORE_COUNT, experiment_id, client_id);
    if (database_query_update(query) == 0) {
        log_error(AT, "Error when decrementing numCores: %s", mysql_error(connection));
        delete[] query;
        return 0;
    }
    delete[] query;
    return 1;
}

/**
 * Executes the queries needed to fetch, lock and update a job to running status
 * of the given experiment. The job details are stored in <code>job</job>.
 * Also updates the job row to indicate which grid (<code>grid_queue_id</code>)
 * the job runs on.
 * 
 * @param grid_queue_id ID of the grid the client runs on
 * @param experiment_id ID of the experiment of which a job should be processed
 * @param job reference to a job instance that will be filled with the job row data
 * @return id of the job > 0 on success, <= 0 on errors or if there are no jobs
 */
int db_fetch_job(int grid_queue_id, int experiment_id, Job& job) {
    mysql_autocommit(connection, 0);
    
    char* query = new char[1024];
    snprintf(query, 1024, LIMIT_QUERY, experiment_id);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute LIMIT_QUERY query");
        // TODO: do something
        delete[] query;
        mysql_autocommit(connection, 1);
        return -1;
    }
    if (mysql_num_rows(result) < 1) {
        mysql_free_result(result);
        delete[] query;
        mysql_autocommit(connection, 1);
        return -1;
    }
    MYSQL_ROW row = mysql_fetch_row(result);
    int limit = atoi(row[0]);
    mysql_free_result(result);
    
    snprintf(query, 1024, SELECT_ID_QUERY, experiment_id, limit);
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute SELECT_ID_QUERY query");
        // TODO: do something
        delete[] query;
        mysql_autocommit(connection, 1);
        return -1;
    }
    if (mysql_num_rows(result) < 1) {
        mysql_free_result(result);
        delete[] query;
        mysql_autocommit(connection, 1);
        return -1;
    }
    row = mysql_fetch_row(result);
    int idJob = atoi(row[0]);
    mysql_free_result(result);

    snprintf(query, 1024, SELECT_FOR_UPDATE, idJob);
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute SELECT_FOR_UPDATE query");
        // TODO: do something
        delete[] query;
        mysql_free_result(result);
        mysql_autocommit(connection, 1);
        return -1;
    }
    if (mysql_num_rows(result) < 1) {
        mysql_free_result(result);
        mysql_commit(connection);
        delete[] query;
        mysql_autocommit(connection, 1);
        return -1; // job was taken by another client between the 2 queries
    }
    row = mysql_fetch_row(result);
    
    job.idJob = atoi(row[0]);
    job.idSolverConfig = atoi(row[1]);
    job.idExperiment = atoi(row[2]);
    job.idInstance = atoi(row[3]);
    job.run = atoi(row[4]);
    if (row[5] != NULL) job.seed = atoi(row[5]); // TODO: not NN column
    job.priority = atoi(row[6]);
    if (row[7] != NULL) job.CPUTimeLimit = atoi(row[7]);
    if (row[8] != NULL) job.wallClockTimeLimit = atoi(row[8]);
    if (row[9] != NULL) job.memoryLimit = atoi(row[9]);
    if (row[10] != NULL) job.stackSizeLimit = atoi(row[10]);
    if (row[11] != NULL) job.outputSizeLimit = atoi(row[11]);
    mysql_free_result(result);
    
    string ipaddress = get_ip_address(false);
    if (ipaddress == "") ipaddress = get_ip_address(true);
    string hostname = get_hostname();
    
    snprintf(query, 1024, LOCK_JOB, grid_queue_id, hostname.c_str(), ipaddress.c_str(), idJob);
    if (database_query_update(query) == 0) {
        log_error(AT, "Couldn't execute LOCK_JOB query");
        // TODO: do something
        delete[] query;
        mysql_autocommit(connection, 1);
        return -1;
    }
    delete[] query;
    mysql_commit(connection);
    mysql_autocommit(connection, 1);;
    return idJob;
}

/**
 * Retrieves the grid queue information of the grid queue with id <code>grid_queue_id</code>
 * and stores them in the given <code>grid_queue</code> instance.
 * 
 * @param grid_queue_id ID of the grid queue
 * @param grid_queue reference to a GridQueue instance
 * @return 1 on success, != 1 on errors/no results
 */
int get_grid_queue_info(int grid_queue_id, GridQueue& grid_queue) {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_GRID_QUEUE_INFO, grid_queue_id);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute query to get grid infos");
        // TODO: do something
        delete[] query;
        return 0;
    }
    delete[] query;
    if (mysql_num_rows(result) != 1) {
        mysql_free_result(result);
        return -1;
    }
    MYSQL_ROW row = mysql_fetch_row(result);
    grid_queue.idgridQueue = grid_queue_id;
    grid_queue.name = row[0];
    grid_queue.location = row[1];
    if (row[2] == NULL) grid_queue.numNodes = 0;
    else grid_queue.numNodes = atoi(row[2]);
    grid_queue.numCPUs = atoi(row[3]);
    grid_queue.walltime = atoi(row[4]);
    grid_queue.availNodes = atoi(row[5]);
    if (row[6] == NULL) grid_queue.maxJobsQueue = 0;
    else grid_queue.maxJobsQueue = atoi(row[6]);
    grid_queue.description = row[7];
    
    mysql_free_result(result);
    return 1;
}

/**
 * Fetches the content of the message column of the client with the given <code>client_id</code>
 * from the Client table and stores it in the passed string reference <code>message</code>.
 * 
 * @param client_id ID of the client
 * @param message reference to a string where the message should be put
 * @return 1 on success, 0 on errors
 */
int get_message(int client_id, string& message, MYSQL* con) {
    mysql_autocommit(con, 0);
    char *query = new char[1024];
    snprintf(query, 1024, LOCK_MESSAGE, client_id);
    MYSQL_RES* result;
    if (database_query_select(query, result, con) == 0) {
        log_error(AT, "Couldn't execute LOCK_MESSAGE query");
        delete[] query;
        mysql_commit(con);
        mysql_autocommit(con, 1);
        return 0;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == NULL) {
        mysql_free_result(result);
        log_error(AT, "Didn't find entry for client in Client table.");
        mysql_commit(con);
        mysql_autocommit(con, 1);
        return 0;
    }
    message = row[0];
    mysql_free_result(result);
    query = new char[1024];
    snprintf(query, 1024, CLEAR_MESSAGE, client_id);
    if (database_query_update(query, con) == 0) {
        log_error(AT, "Couldn't execute CLEAR_MESSAGE query");
        delete[] query;
        mysql_commit(con);
        mysql_autocommit(con, 1);
        return 0;
    }
    delete[] query;
    mysql_commit(con);
    mysql_autocommit(con, 1);
    return 1;
}

/**
 * Fetches the solver for a job.
 * @param job the job
 * @param solver the solver, which will be filled by the database data
 * @return value != 0: success
 */
int get_solver(Job& job, Solver& solver) {
	char *query = new char[1024];
	snprintf(query, 1024, QUERY_SOLVER, job.idSolverConfig);
	MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_SOLVER query");
        // TODO: do something
        delete[] query;
        return 0;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (mysql_num_rows(result) < 1) {
    	// will never happen
    	mysql_free_result(result);
    	return 0;
    }
    solver.idSolver = atoi(row[0]);
    solver.name = row[1];
    solver.binaryName = row[2];
    solver.md5 = row[3];
    mysql_free_result(result);
    return 1;
}

/**
 * Fetches the instance for a job.
 * @param job the job
 * @param instance the instance, which will be filled by the database data
 * @return value != 0: success
 */
int get_instance(Job& job, Instance& instance) {
	char *query = new char[1024];
	snprintf(query, 1024, QUERY_INSTANCE, job.idInstance);
	MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_INSTANCE query");
        delete[] query;
        return 0;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (mysql_num_rows(result) < 1) {
    	// will never happen
    	mysql_free_result(result);
    	return 0;
    }
    instance.idInstance = job.idInstance;
    instance.name = row[0];
    instance.md5 = row[1];
    mysql_free_result(result);
    return 1;
}

/**
 * Tries to update the instance lock.<br/>
 * @param instance the instance for which the lock should be updated
 * @param fsid the file system id
 * @return value != 0: success
 */
int update_instance_lock(Instance& instance, int fsid) {
	char *query = new char[1024];
	snprintf(query, 1024, QUERY_UPDATE_INSTANCE_LOCK, instance.idInstance, fsid);
    if (database_query_update(query) == 0) {
        log_error(AT, "Couldn't execute QUERY_UPDATE_INSTANCE_LOCK query");
        // TODO: do something
        delete[] query;
        return 0;
    }
    delete[] query;
    return mysql_affected_rows(connection) == 1;
}

/**
 * Tries to update the solver lock.<br/>
 * @param solver the solver for which the lock should be updated
 * @param fsid the file system id
 * @return value != 0: success
 */
int update_solver_lock(Solver& solver, int fsid) {
	char *query = new char[1024];
	snprintf(query, 1024, QUERY_UPDATE_SOLVER_LOCK, solver.idSolver, fsid);
    if (database_query_update(query) == 0) {
        log_error(AT, "Couldn't execute QUERY_UPDATE_SOLVER_LOCK query");
        delete[] query;
        return 0;
    }
    delete[] query;
    return mysql_affected_rows(connection) == 1;
}

/**
 * Checks if the specified instance with the file system id is currently locked by any client.
 * @param instance the instance to be checked
 * @param fsid the file system id
 * @return value != 0: instance is locked
 */
int instance_locked(Instance& instance, int fsid) {
	char *query = new char[1024];
	snprintf(query, 1024, QUERY_CHECK_INSTANCE_LOCK, instance.idInstance, fsid);
	MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_CHECK_INSTANCE_LOCK query");
        delete[] query;
        return 1;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (mysql_num_rows(result) < 1) {
    	mysql_free_result(result);
    	return 0;
    }
    int timediff = atoi(row[0]);
    mysql_free_result(result);
    return timediff <= DOWNLOAD_TIMEOUT;
}

/**
 * Checks if the specified solver with the file system id is currently locked by any client.
 * @param solver the solver to be checked
 * @param fsid the file system id
 * @return value != 0: solver is locked
 */
int solver_locked(Solver& solver, int fsid) {
	char *query = new char[1024];
	snprintf(query, 1024, QUERY_CHECK_SOLVER_LOCK, solver.idSolver, fsid);
	MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_CHECK_SOLVER_LOCK query");
        delete[] query;
        return 1;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == NULL) {
    	mysql_free_result(result);
    	return 0;
    }
    int timediff = atoi(row[0]);
    mysql_free_result(result);
    return timediff <= DOWNLOAD_TIMEOUT;
}

/**
 * Locks an instance.<br/>
 * <br/>
 * On success it is guaranteed that this instance was locked.
 * @param instance the instance which should be locked
 * @param fsid the file system id which should be locked for this instance
 * @return value != 0: success
 */
int lock_instance(Instance& instance, int fsid) {
	mysql_autocommit(connection, 0);

	// this query locks the entry with (idInstance, fsid) if existant
	// this is needed to determine if the the client which locked this instance is dead
	//  => only one client should check this and update the lock
	char *query = new char[1024];
	snprintf(query, 1024, QUERY_CHECK_INSTANCE_LOCK, instance.idInstance, fsid);
	MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_CHECK_INSTANCE_LOCK query");
        delete[] query;
        mysql_autocommit(connection, 1);
        return 0;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == NULL) {
        // instance is currently not locked by another client
        // try to create a lock
    	mysql_autocommit(connection, 1);
    	mysql_free_result(result);
    	query = new char[1024];
    	snprintf(query, 1024, QUERY_LOCK_INSTANCE, instance.idInstance, fsid);
        if (database_query_update(query) == 0) {
            // ER_DUP_ENTRY is ok -> was locked by another client
            if (mysql_errno(connection) != ER_DUP_ENTRY) {
                log_error(AT, "Couldn't execute QUERY_LOCK_INSTANCE query");
            }
            delete[] query;
            return 0;
        }
        delete[] query;
        // success
        return 1;
    } else if (atoi(row[0]) > DOWNLOAD_TIMEOUT) {
        // instance was locked by another client but DOWNLOAD_TIMEOUT reached
        // try to update the instance lock => steal lock from dead client
        // might fail if another client was in the same situation (before the row lock) and faster
    	mysql_free_result(result);
    	int res = update_instance_lock(instance, fsid);
    	mysql_autocommit(connection, 1);
    	return res;
    }
    mysql_free_result(result);
    mysql_autocommit(connection, 1);
    return 0;
}

/**
 * Locks a solver.<br/>
 * <br/>
 * On success it is guaranteed that this solver was locked.
 * @param solver the solver which should be locked
 * @param fsid the file system id which should be locked for this solver
 * @return value != 0: success
 */
int lock_solver(Solver& solver, int fsid) {
	mysql_autocommit(connection, 0);

	char *query = new char[1024];
	snprintf(query, 1024, QUERY_CHECK_SOLVER_LOCK, solver.idSolver, fsid);
	MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_CHECK_SOLVER_LOCK query");
        delete[] query;
        mysql_autocommit(connection, 1);
        return 0;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == NULL) {
    	mysql_autocommit(connection, 1);
    	mysql_free_result(result);
    	query = new char[1024];
    	snprintf(query, 1024, QUERY_LOCK_SOLVER, solver.idSolver, fsid);
        if (database_query_update(query) == 0) {
            // ER_DUP_ENTRY is ok -> was locked by another client
            if (mysql_errno(connection) != ER_DUP_ENTRY) {
                log_error(AT, "Couldn't execute QUERY_LOCK_SOLVER query");
            }
            delete[] query;
            return 0;
        }
        return 1;
    } else if (atoi(row[0]) > DOWNLOAD_TIMEOUT) {
    	mysql_free_result(result);
    	int res = update_solver_lock(solver, fsid);
    	mysql_autocommit(connection, 1);
    	return res;
    }
    mysql_free_result(result);
    mysql_autocommit(connection, 1);
    return 0;
}

/**
 * Removes the instance lock.
 * @param instance the instance for which the lock should be removed
 * @param fsid the file system id which was locked
 * @return value != 0: success
 */
int unlock_instance(Instance& instance, int fsid) {
	char *query = new char[1024];
	snprintf(query, 1024, QUERY_UNLOCK_INSTANCE, instance.idInstance, fsid);
    if (database_query_update(query) == 0) {
        log_error(AT, "Couldn't execute QUERY_UNLOCK_INSTANCE query");
        delete[] query;
        return 0;
    }
    delete[] query;
    return 1;
}

/**
 * Removes the solver lock.
 * @param solver the solver for which the lock should be removed
 * @param fsid the file system id which was locked
 * @return value != 0: success
 */
int unlock_solver(Solver& solver, int fsid) {
	char *query = new char[1024];
    snprintf(query, 1024, QUERY_UNLOCK_SOLVER, solver.idSolver, fsid);
    if (database_query_update(query) == 0) {
        log_error(AT, "Couldn't execute QUERY_UNLOCK_SOLVER query");
        delete[] query;
        return 0;
    }
    delete[] query;
    return 1;
}

/**
 * Downloads the instance binary.
 * @param instance binary of this instance will be downloaded
 * @param instance_binary name of file where the data should be stored
 * @return value != 0: success
 */
int db_get_instance_binary(Instance& instance, string& instance_binary) {
	// receive instance binary
	log_message(LOG_DEBUG, "receiving instance: %s", instance_binary.c_str());
	char *query = new char[1024];
	snprintf(query, 1024, QUERY_INSTANCE_BINARY, instance.idInstance);
	MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_INSTANCE_BINARY query");
        // TODO: do something
        delete[] query;
        return 0;
    }
    delete[] query;

    MYSQL_ROW row;
    if ((row = mysql_fetch_row(result)) == NULL) {
    	mysql_free_result(result);
    	return 0;
    }

	unsigned long *lengths = mysql_fetch_lengths(result);

	char* data = (char *) malloc(lengths[0] * sizeof(char));
	memcpy(data, row[0], lengths[0]);
	int res = copy_data_to_file(instance_binary, data, lengths[0], 0666);
	mysql_free_result(result);
	free(data);
	return res;
}

/**
 * Downloads the solver binary.
 * @param solver binary of this solver will be downloaded
 * @param solver_binary name of file where the data should be stored
 * @return value != 0: success
 */
int db_get_solver_binary(Solver& solver, string& solver_binary) {
	// receive solver binary
    log_message(LOG_DEBUG, "receiving solver: %s", solver_binary.c_str());
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_SOLVER_BINARY, solver.idSolver);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_SOLVER_BINARY query");
        delete[] query;
        return 0;
    }
    delete[] query;

    MYSQL_ROW row;
    if ((row = mysql_fetch_row(result)) == NULL) {
        mysql_free_result(result);
        return 0;
    }

    unsigned long *lengths = mysql_fetch_lengths(result);

    char* data = (char *) malloc(lengths[0] * sizeof(char));
    memcpy(data, row[0], lengths[0]);
    int res = copy_data_to_file(solver_binary, data, lengths[0], 0777);
    mysql_free_result(result);
    free(data);
    return res;
}

/**
 * Updates the instance lock until finished is true in the <code>Instance_lock_update</code> data structure.<br/>
 * <br/>
 * Will not delete the instance lock.
 * @param ptr pointer to <code>Instance_lock_update</code> data structure.
 */
void *update_instance_lock(void* ptr) {
	Instance_lock_update* ilu = (Instance_lock_update*) ptr;

	// create connection
    MYSQL* con = NULL;
    if (!get_new_connection(con)) {
        log_error(AT, "[update_instance_lock:thread] Database connection attempt failed: %s", mysql_error(con));
        return NULL;
    }
    // prepare query
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_UPDATE_INSTANCE_LOCK, ilu->instance->idInstance, ilu->fsid);

    int qtime = 0;
    while (!ilu->finished) {
        if (qtime <= 0) {
            // update lastReport entry for this instance
            if (!database_query_update(query, con)) {
                log_error(AT, "[update_instance_lock:thread] Couldn't execute QUERY_UPDATE_INSTANCE_LOCK query");
                // TODO: do something
                delete[] query;
                return NULL;
            }
            qtime = DOWNLOAD_REFRESH;
        }
        sleep(1);
        qtime--;
    }
    delete[] query;
    mysql_close(con);
    log_message(LOG_DEBUG, "[update_instance_lock:thread] Closed database connection");
    return NULL;
}

/**
 * Updates the solver lock until finished is true in the <code>Solver_lock_update</code> data structure.<br/>
 * <br/>
 * Will not delete the solver lock.
 * @param ptr pointer to <code>Solver_lock_update</code> data structure.
 */
void *update_solver_lock(void* ptr) {
    Solver_lock_update* slu = (Solver_lock_update*) ptr;

    // create a new connection
    MYSQL* con = NULL;
    if (!get_new_connection(con)) {
        log_error(AT, "[update_solver_lock:thread] Database connection attempt failed: %s", mysql_error(con));
        return NULL;
    }
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_UPDATE_SOLVER_LOCK, slu->solver->idSolver, slu->fsid);
    int qtime = 0;

    // the parent will set finished to true
    while (!slu->finished) {
        if (qtime <= 0) {
            // update the lastReport entry in the database
            if (!database_query_update(query, con)) {
                log_error(AT, "[update_solver_lock:thread] Couldn't execute QUERY_UPDATE_SOLVER_LOCK query");
                // TODO: do something
                delete[] query;
                return NULL;
            }
            qtime = DOWNLOAD_REFRESH;
        }
        sleep(1);
        qtime--;
    }
    // clean exit
    delete[] query;
    mysql_close(con);
    log_message(LOG_DEBUG, "[update_solver_lock:thread] Closed database connection");
    return NULL;
}

/**
 * Tries to get the binary of the specified instance. Makes sure that no other client is downloading<br/>
 * this binary on the same file system (specified by <code>fsid</code>) at the same time.<br/>
 * <br/>
 * On success, the <code>instance_binary</code> contains the file name of the instance and it is<br/>
 * guaranteed that the md5 sum matches the md5 sum in the database.
 * @param instance the solver for which the binary should be downloaded
 * @param instance_binary contains the file name to the binary after download and is set by this function
 * @param fsid the unique <code>fsid</code> on which no other client should download this instance at the same time
 * @return value > 0: success
 */
int get_instance_binary(Instance& instance, string& instance_binary, int fsid) {
	instance_binary = instance_path + "/" + instance.md5 + "_" + instance.name;
	log_message(LOG_DEBUG, "getting instance %s", instance_binary.c_str());
	if (file_exists(instance_binary) && check_md5sum(instance_binary, instance.md5)) {
		log_message(LOG_DEBUG, "instance exists and md5 check was ok.");
		return 1;
	}
	log_message(LOG_DEBUG, "instance doesn't exist or md5 check was not ok..");
	int got_lock = 0;
	if (!instance_locked(instance, fsid)) {
		log_message(LOG_DEBUG, "trying to lock instance for download");
		if (lock_instance(instance, fsid)) {
			log_message(LOG_DEBUG, "locked! downloading instance..");

			Instance_lock_update ilu;
			ilu.finished = 0;
			ilu.fsid = fsid;
			ilu.instance = &instance;
			pthread_t thread;
			pthread_create( &thread, NULL, update_instance_lock, (void*) &ilu);
			got_lock = 1;
			if (!db_get_instance_binary(instance, instance_binary)) {
                log_error(AT, "Could not receive instance binary.");
            }

			if (is_lzma(instance_binary)) {
				log_message(LOG_DEBUG, "Extracting instance..");
				string instance_binary_lzma = instance_binary + ".lzma";
				rename(instance_binary.c_str(), (instance_binary + ".lzma").c_str());
				int res = lzma_extract(instance_binary_lzma, instance_binary);
				remove(instance_binary_lzma.c_str());
				if (!res) {
					log_error(AT, "Could not extract %s.", instance_binary_lzma.c_str());
					return 0;
				}
				log_message(LOG_DEBUG, "..done.");
			}
			ilu.finished = 1;
			pthread_join(thread, NULL);

			unlock_instance(instance, fsid);
			log_message(LOG_DEBUG, "..done.");
		}
	}
	if (!got_lock) {
		while (instance_locked(instance, fsid)) {
			log_message(LOG_DEBUG, "waiting for instance download from other client: %s", instance_binary.c_str());
			sleep(DOWNLOAD_REFRESH);
		}
	}
	if (!check_md5sum(instance_binary, instance.md5)) {
		log_message(LOG_DEBUG, "md5 check failed. giving up.");
		return 0;
	}
	return 1;
}

/**
 * Tries to get the binary of the specified solver. Makes sure that no other client is downloading<br/>
 * this binary on the same file system (specified by <code>fsid</code>) at the same time.<br/>
 * <br/>
 * On success, the <code>solver_binary</code> contains the file name of the solver and it is<br/>
 * guaranteed that the md5 sum matches the md5 sum in the database.
 * @param solver the solver for which the binary should be downloaded
 * @param solver_binary contains the file name to the binary after download and is set by this function
 * @param fsid the unique <code>fsid</code> on which no other client should download this solver at the same time
 * @return value > 0: success
 */
int get_solver_binary(Solver& solver, string& solver_binary, int fsid) {
	solver_binary = solver_path + "/" + solver.md5 + "_" + solver.binaryName;

	log_message(LOG_DEBUG, "getting solver %s", solver_binary.c_str());
	if (file_exists(solver_binary) && check_md5sum(solver_binary, solver.md5)) {
		log_message(LOG_DEBUG, "solver exists and md5 check was ok.");
		return 1;
	}
	log_message(LOG_DEBUG, "solver doesn't exist or md5 check was not ok..");
	int got_lock = 0;
	if (!solver_locked(solver, fsid)) {
		log_message(LOG_DEBUG, "trying to lock solver for download");
		if (lock_solver(solver, fsid)) {
			log_message(LOG_DEBUG, "locked! downloading solver..");
			Solver_lock_update slu;
			slu.finished = 0;
			slu.fsid = fsid;
			slu.solver = &solver;
			pthread_t thread;
			pthread_create( &thread, NULL, update_solver_lock, (void*) &slu);
			got_lock = 1;
			if (!db_get_solver_binary(solver, solver_binary)) {
                log_error(AT, "Could not receive solver binary.");
            }
			slu.finished = 1;
			pthread_join(thread, NULL);

			unlock_solver(solver, fsid);
			log_message(LOG_DEBUG, "..done.");
		}
	}
	if (!got_lock) {
		while (solver_locked(solver, fsid)) {
			log_message(LOG_DEBUG, "waiting for solver download from other client: %s", solver_binary.c_str());
			sleep(DOWNLOAD_REFRESH);
		}
	}
	if (!check_md5sum(solver_binary, solver.md5)) {
		log_message(LOG_DEBUG, "md5 check failed. giving up.");
		return 0;
	}
	return 1;
}

/**
 * Retrieves the parameters of the solver configuration specified by <code>solver_config_id</code>.
 * The parameters are put into the the passed vector of Parameter instances.
 * 
 * @param solver_config_id ID of the solver configuration
 * @param params reference to an (empty) vector of Parameters where the parameters are put in.
 * @return 1 on success, 0 on errors
 */
int get_solver_config_params(int solver_config_id, vector<Parameter>& params) {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_SOLVER_CONFIG_PARAMS, solver_config_id);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute query to get solver config params: %s", mysql_error(connection));
        // TODO: do something
        delete[] query;
        return 0;
    }
    delete[] query;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != NULL) {
        Parameter param;
        param.idParameter = atoi(row[0]);
        param.name = row[1];
        if (row[2] == NULL) param.prefix = "";
        else param.prefix = row[2];
        param.hasValue = atoi(row[3]) != 0;
        if (row[4] == NULL) param.defaultValue = "";
        else param.defaultValue = row[4];
        param.order = atoi(row[5]);
        if (row[6] == NULL) param.value = "";
        else param.value = row[6];
        params.push_back(param);
    }
    mysql_free_result(result);
    return 1;
}

/**
 * Updates a job row with the data from the passed <code>job</code>.
 * BLOBs are escaped using <code>mysql_real_escape</code>.
 * 
 * @param job The job of which the corresponding database row should be updated.
 * @return 1 on success, 0 on errors
 */
int db_update_job(const Job& job) {
	char* escaped_solver_output = new char[job.solverOutput_length * 2 + 1];
    if (escaped_solver_output == 0) {
        log_error(AT, "Ran out of memory when allocating memory for output data!");
    }
	mysql_real_escape_string(connection, escaped_solver_output, job.solverOutput, job.solverOutput_length);
    
	char* escaped_launcher_output = new char[job.launcherOutput.length() * 2 + 1];
    if (escaped_launcher_output == 0) {
        log_error(AT, "Ran out of memory when allocating memory for output data!");
    }
	mysql_real_escape_string(connection, escaped_launcher_output, job.launcherOutput.c_str(), job.launcherOutput.length());
    
	char* escaped_verifier_output = new char[job.verifierOutput_length * 2 + 1];
    if (escaped_verifier_output == 0) {
        log_error(AT, "Ran out of memory when allocating memory for output data!");
    }
	mysql_real_escape_string(connection, escaped_verifier_output, job.verifierOutput, job.verifierOutput_length);
    
	char* escaped_watcher_output = new char[job.watcherOutput.length() * 2 + 1];
    if (escaped_watcher_output == 0) {
        log_error(AT, "Ran out of memory when allocating memory for output data!");
    }
	mysql_real_escape_string(connection, escaped_watcher_output, job.watcherOutput.c_str(), job.watcherOutput.length());
    
    unsigned long total_length = job.solverOutput_length * 2 + 1 +
                                job.launcherOutput.length() * 2 + 1 +
                                job.verifierOutput_length * 2 + 1 +
                                job.watcherOutput.length() * 2 + 1 +
                                4096;
    char* query_job = new char[total_length];
    int queryLength = snprintf(query_job, total_length, QUERY_UPDATE_JOB, job.status,
        job.resultCode, job.resultTime, escaped_solver_output, escaped_watcher_output,
        escaped_launcher_output, escaped_verifier_output, job.solverExitCode,
        job.watcherExitCode, job.verifierExitCode, job.idJob, job.idJob);

    int status = mysql_real_query(connection, query_job, queryLength + 1);
    if (status != 0) {
        if (mysql_errno(connection) == CR_SERVER_GONE_ERROR || mysql_errno(connection) == CR_SERVER_LOST) {
            for (int i = 0; i < opt_wait_jobs_time / WAIT_BETWEEN_RECONNECTS; i++) {
                sleep(WAIT_BETWEEN_RECONNECTS);
                if (mysql_real_query(connection, query_job, queryLength + 1) != 0) {
                    // still doesn't work
                    log_error(AT, "Lost connection to server and couldn't \
                            reconnect when executing job update query: %s", mysql_error(connection));
                } else {
                    // successfully re-issued query
                    log_message(LOG_INFO, "Lost connection but successfully re-established \
                                when executing job update query");
                    delete[] escaped_solver_output;
                    delete[] escaped_launcher_output;
                    delete[] escaped_verifier_output;
                    delete[] escaped_watcher_output;
                    delete[] query_job;
                    return 1;
                }
            }
        }
        log_error(AT, "DB update query error: %s errno: %d", mysql_error(connection), mysql_errno(connection));
        delete[] escaped_solver_output;
        delete[] escaped_launcher_output;
        delete[] escaped_verifier_output;
        delete[] escaped_watcher_output;
        delete[] query_job;
        return 0;
    }
    
    delete[] escaped_solver_output;
    delete[] escaped_launcher_output;
    delete[] escaped_verifier_output;
    delete[] escaped_watcher_output;
    delete[] query_job;
    return 1;
}
