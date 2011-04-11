#include <string>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <map>
#include <mysql/mysql.h>
#include <mysql/my_global.h>
#include <mysql/errmsg.h>

#include "host_info.h"
#include "database.h"
#include "log.h"

using std::string;
using std::vector;
using std::map;

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
        log_error(AT, "Database connection attempt failed: %s", mysql_error(connection));
        return 0;
    }
    
    // enable auto-reconnect on SERVER_LOST and SERVER_GONE errors
    // e.g. due to connection time outs. Failed queries have to be
    // re-issued in any case.
    int mysql_opt_reconnect = 1;
	mysql_options(connection, MYSQL_OPT_RECONNECT, &mysql_opt_reconnect);
    
    log_message(LOG_INFO, "Established database connection to %s:%s@%s:%u/%s",
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
int database_query_select(string query, MYSQL_RES*& res) {
    int status = mysql_query(connection, query.c_str());
    if (status != 0) {
		if (status == CR_SERVER_GONE_ERROR || status == CR_SERVER_LOST) {
			// server connection lost, try to re-issue query once
			if (mysql_query(connection, query.c_str()) != 0) {
				// still doesn't work
				log_error(AT, "Lost connection to server and couldn't \
							reconnect when executing query: %s - %s",
                            query.c_str(), mysql_error(connection));
				return 0;
			}
			else {
				// successfully re-issued query
				log_message(LOG_INFO, "Lost connection but successfully re-established \
								when executing query: %s", query.c_str());
				return 1;
			}
		}
		
        log_error(AT, "Query failed: %s", query.c_str());
        return 0; 
    }
    
    if ((res = mysql_store_result(connection)) == NULL) {
        log_error(AT, "Couldn't fetch query result of query: %s - %s",
                    query.c_str(), mysql_error(connection));
        return 0;
    }
    
    return 1;
}

/**
 * Issues an insert/update query. If the database connection timed out this function will
 * attempt to re-issue the query once.
 * 
 * The query may not contain any null bytes.
 * 
 * Returns 0 on errors, 1 on success.
 */
int database_query_update(string query) {
    int status = mysql_query(connection, query.c_str());
    if (status != 0) {
		if (status == CR_SERVER_GONE_ERROR || status == CR_SERVER_LOST) {
			// server connection lost, try to re-issue query once
			if (mysql_query(connection, query.c_str()) != 0) {
				// still doesn't work
				log_error(AT, "Lost connection to server and couldn't \
							reconnect when executing query: %s - %s",
                            query.c_str(), mysql_error(connection));
				return 0;
			}
			else {
				// successfully re-issued query
				log_message(LOG_INFO, "Lost connection but successfully re-established \
								when executing query: %s", query.c_str());
				return 1;
			}
		}
		
        log_error(AT, "Query failed: %s", query.c_str());
        return 0; 
    }

    return 1;
}

/**
 * Closes the database connection.
 */
void database_close() {
	mysql_close(connection);
	log_message(LOG_INFO, "Closed database connection");
}

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
    job.seed = atoi(row[5]);
    job.priority = atoi(row[6]);
    job.CPUTimeLimit = atoi(row[7]);
    job.wallClockTimeLimit = atoi(row[8]);
    job.memoryLimit = atoi(row[9]);
    job.stackSizeLimit = atoi(row[10]);
    job.outputSizeLimit = atoi(row[11]);
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
