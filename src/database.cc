#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <map>
#include <cmath>
#include <mysql/mysql.h>
#include <mysql/my_global.h>
#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>
#include <pthread.h>
#include <arpa/inet.h>


#include "host_info.h"
#include "database.h"
#include "database_fs_locking.h"
#include "log.h"
#include "file_routines.h"
#include "lzma.h"
#include "unzip/miniunz.h"
#include "jobserver.h"

using std::string;
using std::vector;
using std::map;
using std::stringstream;

extern string base_path;
extern string solver_path;
extern string instance_path;
extern string cost_binary_path;
extern string download_path;
extern string solver_download_path;
extern string instance_download_path;
extern string verifier_path;
extern string verifier_download_path;
extern string cost_binary_download_path;

MYSQL* connection = 0;

// from client.cc
extern time_t opt_wait_jobs_time; // seconds

static time_t WAIT_BETWEEN_RECONNECTS = 5;

// the file system id. Assigned when client signs on.
int fsid;

// this will be set if the alternative fetch job id method is used
Jobserver* jobserver = NULL;

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
int database_connect(const string& hostname, const string& database, const string& username, const string& password,
        unsigned int port) {
    if (connection != 0)
        mysql_close(connection);


    connection = mysql_init(NULL);
    if (connection == NULL) {
        return 0;
    }

    if (mysql_real_connect(connection, hostname.c_str(), username.c_str(), password.c_str(), database.c_str(), port,
            NULL, 0) == NULL) {
        log_error(AT, "Database connection attempt failed: %s", mysql_error(connection));
        return 0;
    }

    // enable auto-reconnect on SERVER_LOST and SERVER_GONE errors
    // e.g. due to connection time outs. Failed queries have to be
    // re-issued in any case.
    my_bool mysql_opt_reconnect = 1;
    mysql_options(connection, MYSQL_OPT_RECONNECT, &mysql_opt_reconnect);

    log_message(LOG_INFO, "Established database connection to %s:%s@%s:%u/%s", username.c_str(), password.c_str(),
            hostname.c_str(), port, database.c_str());

    return 1;
}

/**
 * Establishes a new database connection by using the fields provided in the current database connection.
 * This fails if no connection was established.
 * @param con the new connection
 */
int get_new_connection(MYSQL *&con) {
    if (con != NULL)
        mysql_close(con);
    con = mysql_init(NULL);
    if (con == NULL || connection == NULL) {
        return 0;
    }
    if (mysql_real_connect(con, connection->host, connection->user, connection->passwd, connection->db,
            connection->port, NULL, 0) == NULL) {
        log_error(AT, "Database connection attempt failed: %s", mysql_error(con));
        return 0;
    }
    // enable auto-reconnect on SERVER_LOST and SERVER_GONE errors
    // e.g. due to connection time outs. Failed queries have to be
    // re-issued in any case.
    my_bool mysql_opt_reconnect = 1;
    mysql_options(connection, MYSQL_OPT_RECONNECT, &mysql_opt_reconnect);

    return 1;
}

string get_db_host() {
    return connection->host;
}

string get_db_username() {
    return connection->user;
}

string get_db_password() {
    return connection->passwd;
}

string get_db() {
    return connection->db;
}

int get_db_port() {
    return connection->port;
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
                    log_error(
                            AT,
                            "Lost connection to server and couldn't \
                            reconnect when executing query: %s - %s",
                            query.c_str(), mysql_error(con));
                } else {
                    // successfully re-issued query
                    log_message(
                            LOG_INFO,
                            "Lost connection but successfully re-established \
                                when executing query: %s",
                            query.c_str());
                    return 1;
                }

            }
            return 0;
        }

        log_error(AT, "Query failed: %s, return code (status): %d errno: %d", query.c_str(), status, mysql_errno(con));
        return 0;
    }

    if ((res = mysql_store_result(con)) == NULL) {
        log_error(AT, "Couldn't fetch query result of query: %s - %s", query.c_str(), mysql_error(con));
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
                    log_error(
                            AT,
                            "Lost connection to server and couldn't \
                            reconnect when executing query: %s - %s",
                            query.c_str(), mysql_error(con));
                } else {
                    // successfully re-issued query
                    log_message(
                            LOG_INFO,
                            "Lost connection but successfully re-established \
                                when executing query: %s",
                            query.c_str());
                    return 1;
                }
            }
            return 0;
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

bool is_recoverable_error() {
    return mysql_errno(connection) == ER_LOCK_DEADLOCK || mysql_errno(connection) == ER_LOCK_WAIT_TIMEOUT;
}

std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    stringstream ss(s);
    string item;
    while(std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

int get_current_model_version() {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_CURRENT_MODEL_VERSION);
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
    int version = atoi(row[0]);
    mysql_free_result(result);
    return version;
}

/**
 * Executes the query needed to insert a new row into the Client table
 * and returns the auto-incremented ID of it. Also determines the file system id
 * or assigns the client id as file system id if it is an unknown file system.
 * 
 * @return id > 0 on success, 0 on errors
 */
int insert_client(const HostInfo& host_info, int grid_queue_id, int jobs_wait_time, string& opt_walltime) {
    // parse walltime
    int walltime = 0;
    if (opt_walltime != "") {
        vector<string> tokens;
        split(opt_walltime, ':', tokens);
        if (tokens.size() == 0 || tokens.size() > 4) {
            log_message(LOG_IMPORTANT, "Unknown walltime format: %s. Expected [[[d:]h:]m:]s");
            return 0;
        }
        int i = 0;
        vector<string>::reverse_iterator it;
        for (it = tokens.rbegin(); it < tokens.rend(); ++it) {
            switch (i) {
                case 0:
                    walltime += atoi((*it).c_str());
                    break;
                case 1:
                    walltime += 60*atoi((*it).c_str());
                    break;
                case 2:
                    walltime += 3600*atoi((*it).c_str());
                    break;
                case 3:
                    walltime += 86400*atoi((*it).c_str());
                    break;
            }
            i++;
        }
    }
    unsigned int tries = 0;
    bool first_try = true;
    while (first_try || tries++ < max_recover_tries) {
        first_try = false;
        char* query = new char[32768];
        snprintf(query, 32768, QUERY_LOCK_CLIENT_FS);
        mysql_autocommit(connection, 0);
        MYSQL_RES* result = 0;
        if (database_query_select(query,result) == 0) {
            delete[] query;
            log_error(AT, "Error while locking client table: %s", mysql_error(connection));
            if (is_recoverable_error() && ++tries < max_recover_tries) {
                mysql_autocommit(connection, 1);
                continue;
            }
            mysql_autocommit(connection, 1);
            return 0;
        }
        MYSQL_ROW row;
        int res = -1;
        if ((row = mysql_fetch_row(result))) {
            res = atoi(row[0]);
        }
        mysql_free_result(result);

        if (res == -1) {
            delete[] query;
            mysql_autocommit(connection,1);
            log_error(AT, "Could not lock client fs: %s", mysql_error(connection));
            return 0;
        } else if (res != 1) {
            log_error(AT, "Could not lock client fs: %s", mysql_error(connection));
            delete[] query;
            mysql_autocommit(connection, 1);
            continue;
        }

        snprintf(query, 32768, QUERY_INSERT_CLIENT, host_info.num_cores, host_info.num_threads,
                host_info.hyperthreading, host_info.turboboost, host_info.cpu_model.c_str(), host_info.cache_size,
                host_info.cpu_flags.c_str(), host_info.memory, host_info.free_memory, host_info.cpuinfo.c_str(),
                host_info.meminfo.c_str(), "", grid_queue_id, jobs_wait_time, walltime);

        int id = -1;

        if (database_query_update(query) == 1) {
            id = mysql_insert_id(connection);
        }

        if (id == -1) {
            log_error(AT, "Error when inserting into client table: %s", mysql_error(connection));
            delete[] query;
            if (is_recoverable_error() && ++tries < max_recover_tries) {
                mysql_autocommit(connection, 1);
                continue;
            }
            mysql_autocommit(connection, 1);
            return 0;
        }

        // setup file system
        // TODO: check if something fails
        if (file_exists(download_path + "/fsid")) {
            fstream f((download_path + "/fsid").c_str(), fstream::in);
            f >> fsid;
            f.close();
        } else {
            fstream f((download_path + "/fsid").c_str(), fstream::out);
            f << id;
            f.close();
            fsid = id;
        }
        create_directory(instance_download_path);
        create_directory(solver_download_path);
        create_directory(verifier_download_path);
        create_directory(cost_binary_download_path);

        snprintf(query, 32768, QUERY_RELEASE_CLIENT_FS);
        bool unlocked = false;
        tries = 0;
        // the next query must not fail
        while (++tries < max_recover_tries) {
            result = 0;
            if (database_query_select(query, result) == 0) {
                log_error(AT, "Error while unlocking client table: %s", mysql_error(connection));
            } else {
                if ((row = mysql_fetch_row(result))) {
                    if (atoi(row[0]) == 1) {
                        unlocked = true;
                        mysql_free_result(result);
                        break;
                    }
                }
            }
            mysql_free_result(result);
        }
        delete[] query;
        mysql_autocommit(connection, 1);
        if (!unlocked) {
            // this will result in an exit of the client and unlock the mutex
            return 0;
        }
        return id;
    }
    return 0;
}

/**
 * Executes the query needed to update the gridQueue table with the host info.
 * 
 * @return 1 on success, 0 on errors
 */
int fill_grid_queue_info(const HostInfo& host_info, int grid_queue_id) {
    char* query = new char[32768];
    snprintf(query, 32768, QUERY_FILL_GRID_QUEUE_INFO, host_info.num_cores, host_info.num_threads,
            host_info.hyperthreading, host_info.turboboost, host_info.cpu_model.c_str(), host_info.cache_size,
            host_info.cpu_flags.c_str(), host_info.memory, host_info.cpuinfo.c_str(), host_info.meminfo.c_str(),
            grid_queue_id);

    unsigned int tries = 0;
    do {
        if (database_query_update(query) == 1) {
            delete[] query;
            return 1;
        }
    } while (is_recoverable_error() && ++tries < max_recover_tries);

    log_error(AT, "Error when updating gridQueue table with host info: %s", mysql_error(connection));
    delete[] query;
    return 0;
}

/**
 * Executes the query needed to delete the client row with the given id from the Client table.
 * @param client_id id of the row that should be deleted
 * @return 1 on success, 0 on errors
 */
int delete_client(int client_id) {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_DELETE_CLIENT, client_id);

    unsigned int tries = 0;
    do {
        if (database_query_update(query) == 1) {
            delete[] query;
            return 1;
        }
    } while (is_recoverable_error() && ++tries < max_recover_tries);

    log_error(AT, "Error when deleting client from table: %s", mysql_error(connection));
    delete[] query;
    return 0;
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
    if (jobserver != NULL) {
        // use the jobserver fetch experiment ids method.
        string ids;
        if (!jobserver->getPossibleExperimentIds(grid_queue_id, ids)) {
            delete[] query;
            return 0;
        }
        if (ids == "") {
            delete[] query;
            return 1;
        }
        snprintf(query, 4096, QUERY_POSSIBLE_EXPERIMENTS_BY_EXPIDS, ids.c_str());
    } else {
        snprintf(query, 4096, QUERY_POSSIBLE_EXPERIMENTS, grid_queue_id);
    }
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
        experiments.push_back(Experiment(atoi(row[0]), row[1], atoi(row[2]),
                row[3] == NULL ? 0 : atoi(row[3]), // solver_output_preserve_first
                row[4] == NULL ? 0 : atoi(row[4]), // solver_output_preserve_last
                row[5] == NULL ? 0 : atoi(row[5]), // watcher_output_preserve_first
                row[6] == NULL ? 0 : atoi(row[6]), // watcher_output_preserve_last
                row[7] == NULL ? 0 : atoi(row[7]), // verifier_output_preserve_first
                row[8] == NULL ? 0 : atoi(row[8]), // verifier_output_preserve_last
                row[3] != NULL, row[5] != NULL, row[7] != NULL, row[9] == NULL ? 0 : atoi(row[9])));
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

    unsigned int tries = 0;
    do {
        if (database_query_update(query) == 1) {
            delete[] query;
            return 1;
        }
    } while (is_recoverable_error() && ++tries < max_recover_tries);

    log_error(AT, "Error when updating numCores: %s", mysql_error(connection));
    delete[] query;
    return 0;
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

    unsigned int tries = 0;
    do {
        if (database_query_update(query) == 1) {
            delete[] query;
            return 1;
        }
    } while (is_recoverable_error() && ++tries < max_recover_tries);

    log_error(AT, "Error when decrementing numCores: %s", mysql_error(connection));
    delete[] query;
    return 0;
}

/**
 * Executes the queries needed to fetch, lock and update a job to running status
 * of the given experiment. The job details are stored in <code>job</job>.
 * Also updates the job row to indicate which grid (<code>grid_queue_id</code>)
 * the job runs on.
 * 
 * @param client_id ID of the client
 * @param grid_queue_id ID of the grid the client runs on
 * @param experiment_id ID of the experiment of which a job should be processed
 * @param job reference to a job instance that will be filled with the job row data
 * @return id of the job > 0 on success, <= 0 on errors or if there are no jobs
 */
int db_fetch_job(int client_id, int grid_queue_id, int experiment_id, Job& job) {
    int idJob = -1;
    char* query = new char[1024];
    MYSQL_RES* result;
    MYSQL_ROW row;
    if (jobserver != NULL) {
        if (!jobserver->getJobId(experiment_id, idJob)) {
            delete[] query;
            return -1;
        }
    } else {
        snprintf(query, 1024, LIMIT_QUERY, experiment_id);
        if (database_query_select(query, result) == 0) {
            log_error(AT, "Couldn't execute LIMIT_QUERY query");
            // TODO: do something
            delete[] query;
            return -1;
        }
        if (mysql_num_rows(result) < 1) {
            mysql_free_result(result);
            delete[] query;
            return -1;
        }
        row = mysql_fetch_row(result);
        int limit = atoi(row[0]);
        mysql_free_result(result);

        snprintf(query, 1024, SELECT_ID_QUERY, experiment_id, limit);
        if (database_query_select(query, result) == 0) {
            log_error(AT, "Couldn't execute SELECT_ID_QUERY query");
            // TODO: do something
            delete[] query;
            return -1;
        }
        if (mysql_num_rows(result) < 1) {
            mysql_free_result(result);
            delete[] query;
            return -1;
        }
        row = mysql_fetch_row(result);
        idJob = atoi(row[0]);
        mysql_free_result(result);
    }

    if (idJob == -1) {
        delete[] query;
        mysql_autocommit(connection, 1);
        return -1;
    }
    
    mysql_autocommit(connection, 0);
    snprintf(query, 1024, SELECT_FOR_UPDATE, idJob);
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute SELECT_FOR_UPDATE query");
        // TODO: do something
        delete[] query;
        mysql_free_result(result);
        mysql_rollback(connection);
        mysql_autocommit(connection, 1);
        return -1;
    }
    if (mysql_num_rows(result) < 1) {
        mysql_free_result(result);
        delete[] query;
        mysql_rollback(connection);
        mysql_autocommit(connection, 1);
        return -1; // job was taken by another client between the 2 queries
    }
    row = mysql_fetch_row(result);

    job.idJob = atoi(row[0]);
    job.idSolverConfig = atoi(row[1]);
    job.idExperiment = atoi(row[2]);
    job.idInstance = atoi(row[3]);
    job.run = atoi(row[4]);
    if (row[5] != NULL)
        job.seed = atoi(row[5]); // TODO: not NN column
    job.priority = atoi(row[6]);
    if (row[7] != NULL)
        job.CPUTimeLimit = atoi(row[7]);
    if (row[8] != NULL)
        job.wallClockTimeLimit = atoi(row[8]);
    if (row[9] != NULL)
        job.memoryLimit = atoi(row[9]);
    if (row[10] != NULL)
        job.stackSizeLimit = atoi(row[10]);

    mysql_free_result(result);

    string ipaddress = get_ip_address(false);
    if (ipaddress == "")
        ipaddress = get_ip_address(true);
    string hostname = get_hostname();

    snprintf(query, 1024, LOCK_JOB, grid_queue_id, hostname.c_str(), ipaddress.c_str(), client_id, idJob);
    if (database_query_update(query) == 0) {
        log_error(AT, "Couldn't execute LOCK_JOB query");
        // TODO: do something
        delete[] query;
        mysql_rollback(connection);
        mysql_autocommit(connection, 1);
        return -1;
    }
    delete[] query;
    mysql_commit(connection);
    mysql_autocommit(connection, 1);
    ;
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
    if (row[1] == NULL)
        grid_queue.location = "";
    else
        grid_queue.location = row[1];
    grid_queue.numCPUs = atoi(row[2]);
    grid_queue.numCPUsPerJob = atoi(row[3]);
    if (row[4] == NULL)
        grid_queue.description = "";
    else
        grid_queue.description = row[4];
    if (row[5] == NULL)
        grid_queue.numCores = 0;
    else
        grid_queue.numCores = atoi(row[5]);
    if (row[6] == NULL)
        grid_queue.cpu_model = "";
    else
        grid_queue.cpu_model = row[6];

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
int get_message(int client_id, int jobs_wait_time, int current_wait_time, string& message, MYSQL* con) {
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
    snprintf(query, 1024, CLEAR_MESSAGE, jobs_wait_time, current_wait_time, client_id);
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
    solver.idSolverBinary = atoi(row[0]);
    solver.solver_name = row[1];
    solver.binaryName = row[2];
    solver.md5 = row[3];
    if (row[4] != NULL)
        solver.runCommand = row[4];
    else
        solver.runCommand = "";
    solver.runPath = row[5];
    solver.idSolver = atoi(row[6]);
    mysql_free_result(result);
    return 1;
}

/**
 * Fetches the verifier details for a given experiment.
 * @param verifier
 * @return value != 0: success
 */
int get_verifier_details(Verifier& verifier, int idExperiment) {
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_VERIFIER, idExperiment);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_VERIFIER query");
        // TODO: do something
        delete[] query;
        return 0;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (mysql_num_rows(result) < 1) {
    	// no verifier specified ?
        mysql_free_result(result);
        return 0;
    }
    verifier.idVerifier = atoi(row[0]);
    verifier.idVerifierConfig = atoi(row[1]);
    verifier.name = row[2];
    verifier.md5 = row[3];
    if (row[4] != NULL)
    	verifier.runCommand = row[4];
    else verifier.runCommand = "";
    verifier.runPath = row[5];
    mysql_free_result(result);

    query = new char[1024];
    snprintf(query, 1024, QUERY_VERIFIER_PARAMETERS, verifier.idVerifierConfig);
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_VERIFIER_PARAMETERS query");
        // TODO: do something
        delete[] query;
        return 0;
    }
    delete[] query;
    while ((row = mysql_fetch_row(result)) != NULL) {
        VerifierParameter param;
        param.idVerifierParameter = atoi(row[0]);
        param.name = row[1];
        if (row[2] == NULL)
            param.prefix = "";
        else
            param.prefix = row[2];
        param.hasValue = atoi(row[3]) != 0;
        if (row[4] == NULL)
            param.defaultValue = "";
        else
            param.defaultValue = row[4];
        param.order = atoi(row[5]);
        if (row[6] == NULL)
            param.space = false;
        else
            param.space = atoi(row[6]) != 0;
        if (row[7] == NULL)
            param.attachToPrevious = false;
        else
            param.attachToPrevious = atoi(row[7]) != 0;
        if (row[8] == NULL)
            param.value = "";
        else
            param.value = row[8];
        verifier.parameters.push_back(param);
    }
    mysql_free_result(result);
    return 1;
}

int get_cost_binary_details(CostBinary& cost_binary, int idSolver, int idCost) {
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_COST_BINARY_DETAILS, idSolver, idCost);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_COST_BINARY query");
        // TODO: do something
        delete[] query;
        return 0;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (mysql_num_rows(result) < 1) {
    	log_message(LOG_IMPORTANT, "No cost binary %d %d", idSolver, idCost);
        mysql_free_result(result);
        return 0;
    }
    cost_binary.idCostBinary = atoi(row[0]);
    cost_binary.Solver_idSolver = atoi(row[1]);
    cost_binary.Cost_idCost = atoi(row[2]);

    if (row[3] != NULL)
    	cost_binary.binaryName = row[3];
    else cost_binary.binaryName = "";

    if (row[4] != NULL)
    	cost_binary.md5 = row[4];
    else cost_binary.md5 = "";

    if (row[5] != NULL)
    	cost_binary.version = row[5];
    else cost_binary.version = "";

    if (row[6] != NULL)
    	cost_binary.runCommand = row[6];
    else cost_binary.runCommand = "";

    if (row[7] != NULL)
    	cost_binary.runPath = row[7];
    else cost_binary.runPath = "";

    if (row[8] != NULL)
    	cost_binary.parameters = row[8];
    else cost_binary.parameters = "";

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
 * Downloads the instance binary.
 * @param instance binary of this instance will be downloaded
 * @param instance_binary name of file where the data should be stored
 * @return value != 0: success
 */
int db_get_instance_binary(Instance& instance, string& instance_binary) {
    // receive instance binary
    if (create_directories(extract_directory(instance_binary)) == 0) {
        log_error(AT, "Couldn't create directories: %s.", extract_directory(instance_binary).c_str());
        return 0;
    }

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
    snprintf(query, 1024, QUERY_SOLVER_BINARY, solver.idSolverBinary);
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
 * Downloads the verifier binary.
 * @param verifier binary of this verifier will be downloaded
 * @param verifier_binary name of file where the data should be stored
 * @return value != 0: success
 */
int db_get_verifier_binary(Verifier& verifier, string& verifier_binary) {
    // receive verifier binary
    log_message(LOG_DEBUG, "receiving verifier: %s", verifier_binary.c_str());
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_VERIFIER_BINARY, verifier.idVerifier);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_VERIFIER_BINARY query");
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
    int res = copy_data_to_file(verifier_binary, data, lengths[0], 0777);
    mysql_free_result(result);
    free(data);
    return res;
}

/**
 * Downloads the cost binary.
 * @param cost_binary binary of this cost binary will be downloaded
 * @param cost_binary_path name of file where the data should be stored
 * @return value != 0: success
 */
int db_get_cost_binary(CostBinary& cost_binary, string& cost_binary_path) {
    // receive cost binary
    log_message(LOG_DEBUG, "receiving cost binary: %s", cost_binary_path.c_str());
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_COST_BINARY, cost_binary.idCostBinary);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_COST_BINARY query");
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
    int res = copy_data_to_file(cost_binary_path, data, lengths[0], 0777);
    mysql_free_result(result);
    free(data);
    return res;
}

/**
 * returns 1 on success
 */
int get_verifier_binary(Verifier& verifier, string& verifier_base_path) {
    verifier_base_path = verifier_path + "/" + verifier.md5;
    string verifier_download_base_path = verifier_download_path + "/" + verifier.md5;
    string verifier_archive_path = verifier_download_base_path + ".zip";
    string verifier_extract_path = verifier_download_base_path + ".extracting";

    log_message(LOG_DEBUG, "getting verifier %s", verifier.name.c_str());
    if (file_exists(verifier_base_path)) {
        log_message(LOG_DEBUG, "verifier binary exists.");
        return 1;
    }
    if (file_exists(verifier_download_base_path)) {
        log_message(LOG_DEBUG, "copying verifier from download path to base path..");
        if (verifier_download_path != verifier_base_path) {
            copy_directory(verifier_download_base_path, verifier_base_path);
        }
        // TODO: use own recursive chmod function
        int exitcode = system((string("chmod -R 777 ") + "\"" + verifier_base_path + "\"").c_str());
        if (exitcode == -1) {
            log_error(AT, "Error executing chmod -R 777 command");
            return 0;
        }
        log_message(LOG_DEBUG, ".. done.");
        return 1;
    }
    log_message(LOG_DEBUG, "verifier doesn't exist");
    int got_lock = 0;
    log_message(LOG_DEBUG, "trying to lock verifier for download");
    int lock_verifier_res;
    unsigned int tries = 0;
    do {
        lock_verifier_res = lock_file(verifier_archive_path);
    } while (lock_verifier_res == -1 && ++tries < max_recover_tries);

    if (lock_verifier_res == 1) {
        log_message(LOG_DEBUG, "locked! downloading verifier..");
        File_lock_update slu;
        slu.finished = 0;
        slu.fsid = fsid;
        slu.filename = verifier_archive_path;
        pthread_t thread;
        pthread_create(&thread, NULL, update_file_lock_thread, (void*) &slu);
        got_lock = 1;
        if (!db_get_verifier_binary(verifier, verifier_archive_path)) {
            log_error(AT, "Could not receive verifier binary archive.");
        }
        bool md5_check = check_md5sum(verifier_archive_path, verifier.md5);
            if (!create_directory(verifier_extract_path)) {
                log_error(AT, "Could not create temporary directory for extraction");
            }
            unsigned char md5[16];
            if (!decompress(verifier_archive_path.c_str(), verifier_extract_path.c_str(), md5)) {
                log_error(AT, "Error occured when decompressing verifier archive");
            } else {
                char md5String[33];
                char* md5StringPtr;
                int i;
                for (i = 0, md5StringPtr = md5String; i < 16; ++i, md5StringPtr += 2)
                    sprintf(md5StringPtr, "%02x", md5[i]);
                md5String[32] = '\0';
                string md5_str(md5String);
                md5_check |= md5_str == verifier.md5;
                if (!md5_check) {
                    log_message(LOG_IMPORTANT, "md5 check of verifier binary archive %s failed.", verifier_archive_path.c_str());
                } else if (!rename(verifier_extract_path, verifier_download_base_path)) {
                    log_error(AT, "Couldn't rename temporary extraction directory");
                } else {
                    // TODO: use own recursive chmod function
                    int exitcode = system((string("chmod -R 777 ") + "\"" + verifier_download_base_path + "\"").c_str());
                    if (exitcode == -1) {
                        log_error(AT, "Error executing chmod -R 777 command");
                        return 0;
                    }
                }
            }


        slu.finished = 1;

        pthread_join(thread, NULL);
        unlock_file(verifier_archive_path);
        log_message(LOG_DEBUG, "..done.");
    } else {
        log_message(LOG_DEBUG, "could not lock verifier, locked by other client");
    }
    if (!got_lock) {
        while (file_locked(verifier_archive_path)) {
            log_message(LOG_DEBUG, "waiting for veriifer download from other client: %s", verifier.name.c_str());
            sleep(DOWNLOAD_REFRESH);
        }

        // final check if the folder is there (with waits for NFS)
        int count = 1;
        while (!file_exists(verifier_download_base_path) && count <= 10) {
            log_message(LOG_DEBUG, "File doesn't exists.. waiting %d / 10", count);
            sleep(1);
            count++;
        }
    }
    if (file_exists(verifier_download_base_path) && !file_exists(verifier_base_path)) {
        log_message(LOG_DEBUG, "copying verifier from download path to base path..");
        copy_directory(verifier_download_base_path, verifier_base_path);
        // TODO: use own recursive chmod function
        int exitcode = system((string("chmod -R 777 ") + "\"" + verifier_base_path + "\"").c_str());
        if (exitcode == -1) {
            log_error(AT, "Error executing chmod -R 777 command");
            return 0;
        }

        log_message(LOG_DEBUG, ".. done.");
    }
    return file_exists(verifier_base_path);
}


/**
 * returns 1 on success
 */
int get_cost_binary(CostBinary& cost_binary, string& cost_binary_base_path) {
	cost_binary_base_path = cost_binary_path + "/" + cost_binary.md5;
    string cost_binary_download_base_path = cost_binary_download_path + "/" + cost_binary.md5;
    string cost_binary_archive_path = cost_binary_download_base_path + ".zip";
    string cost_binary_extract_path = cost_binary_download_base_path + ".extracting";

    log_message(LOG_DEBUG, "getting cost_binary %s", cost_binary.binaryName.c_str());
    if (file_exists(cost_binary_base_path)) {
        log_message(LOG_DEBUG, "cost_binary binary exists.");
        return 1;
    }
    if (file_exists(cost_binary_download_base_path)) {
        log_message(LOG_DEBUG, "copying cost_binary from download path to base path..");
        if (cost_binary_download_path != cost_binary_base_path) {
            copy_directory(cost_binary_download_base_path, cost_binary_base_path);
        }
        // TODO: use own recursive chmod function
        int exitcode = system((string("chmod -R 777 ") + "\"" + cost_binary_base_path + "\"").c_str());
        if (exitcode == -1) {
            log_error(AT, "Error executing chmod -R 777 command");
            return 0;
        }
        log_message(LOG_DEBUG, ".. done.");
        return 1;
    }
    log_message(LOG_DEBUG, "cost_binary doesn't exist");
    int got_lock = 0;
    log_message(LOG_DEBUG, "trying to lock cost_binary for download");
    int lock_cost_binary_res;
    unsigned int tries = 0;
    do {
        lock_cost_binary_res = lock_file(cost_binary_archive_path);
    } while (lock_cost_binary_res == -1 && ++tries < max_recover_tries);

    if (lock_cost_binary_res == 1) {
        log_message(LOG_DEBUG, "locked! downloading cost_binary..");
        File_lock_update slu;
        slu.finished = 0;
        slu.fsid = fsid;
        slu.filename = cost_binary_archive_path;
        pthread_t thread;
        pthread_create(&thread, NULL, update_file_lock_thread, (void*) &slu);
        got_lock = 1;
        if (!db_get_cost_binary(cost_binary, cost_binary_archive_path)) {
            log_error(AT, "Could not receive cost_binary binary archive.");
        }
        bool md5_check = check_md5sum(cost_binary_archive_path, cost_binary.md5);
            if (!create_directory(cost_binary_extract_path)) {
                log_error(AT, "Could not create temporary directory for extraction");
            }
            unsigned char md5[16];
            if (!decompress(cost_binary_archive_path.c_str(), cost_binary_extract_path.c_str(), md5)) {
                log_error(AT, "Error occured when decompressing cost_binary archive");
            } else {
                char md5String[33];
                char* md5StringPtr;
                int i;
                for (i = 0, md5StringPtr = md5String; i < 16; ++i, md5StringPtr += 2)
                    sprintf(md5StringPtr, "%02x", md5[i]);
                md5String[32] = '\0';
                string md5_str(md5String);
                md5_check |= md5_str == cost_binary.md5;
                if (!md5_check) {
                    log_message(LOG_IMPORTANT, "md5 check of cost_binary binary archive %s failed.", cost_binary_archive_path.c_str());
                } else if (!rename(cost_binary_extract_path, cost_binary_download_base_path)) {
                    log_error(AT, "Couldn't rename temporary extraction directory");
                } else {
                    // TODO: use own recursive chmod function
                    int exitcode = system((string("chmod -R 777 ") + "\"" + cost_binary_download_base_path + "\"").c_str());
                    if (exitcode == -1) {
                        log_error(AT, "Error executing chmod -R 777 command");
                        return 0;
                    }
                }
            }


        slu.finished = 1;

        pthread_join(thread, NULL);
        unlock_file(cost_binary_archive_path);
        log_message(LOG_DEBUG, "..done.");
    } else {
        log_message(LOG_DEBUG, "could not lock cost_binary, locked by other client");
    }
    if (!got_lock) {
        while (file_locked(cost_binary_archive_path)) {
            log_message(LOG_DEBUG, "waiting for cost_binary download from other client: %s", cost_binary.binaryName.c_str());
            sleep(DOWNLOAD_REFRESH);
        }

        // final check if the folder is there (with waits for NFS)
        int count = 1;
        while (!file_exists(cost_binary_download_base_path) && count <= 10) {
            log_message(LOG_DEBUG, "File doesn't exists.. waiting %d / 10", count);
            sleep(1);
            count++;
        }
    }
    if (file_exists(cost_binary_download_base_path) && !file_exists(cost_binary_base_path)) {
        log_message(LOG_DEBUG, "copying cost_binary from download path to base path..");
        copy_directory(cost_binary_download_base_path, cost_binary_base_path);
        // TODO: use own recursive chmod function
        int exitcode = system((string("chmod -R 777 ") + "\"" + cost_binary_base_path + "\"").c_str());
        if (exitcode == -1) {
            log_error(AT, "Error executing chmod -R 777 command");
            return 0;
        }

        log_message(LOG_DEBUG, ".. done.");
    }
    return file_exists(cost_binary_base_path);
}

/**
 * Tries to get the binary of the specified instance. Makes sure that no other client is downloading<br/>
 * this binary on the same file system at the same time.<br/>
 * <br/>
 * On success, the <code>instance_binary</code> contains the file name of the instance and it is<br/>
 * guaranteed that the md5 sum matches the md5 sum in the database.
 * @param instance the solver for which the binary should be downloaded
 * @param instance_binary contains the file name to the binary after download and is set by this function
 * @return value > 0: success
 */
int get_instance_binary(Instance& instance, string& instance_binary) {
    instance_binary = instance_path + "/" + instance.md5 + "/" + instance.name;
    log_message(LOG_DEBUG, "getting instance %s", instance_binary.c_str());
    if (file_exists(instance_binary) && check_md5sum(instance_binary, instance.md5)) {
        log_message(LOG_DEBUG, "instance exists and md5 check was ok.");
        return 1;
    }
    log_message(LOG_DEBUG, "instance doesn't exist in base path or md5 check was not ok..");
    string instance_download_binary = instance_download_path + "/" + instance.md5 + "/" + instance.name;
    if (file_exists(instance_download_binary) && check_md5sum(instance_download_binary, instance.md5)) {
        log_message(LOG_DEBUG, "copying instance binary from download path to base path..");
        if (instance_download_binary != instance_binary && copy_file(instance_download_binary, instance_binary) == 0) {
            log_error(AT, "Could not copy instance binary. Insufficient rights?");
            return 0;
        }
        if (!check_md5sum(instance_binary, instance.md5)) {
            log_message(LOG_DEBUG, "MD5 check failed for copied instance.");
            return 0;
        }
        return 1;
    }
    int got_lock = 0;
    log_message(LOG_DEBUG, "trying to lock instance for download");
    int lock_instance_res;
    unsigned int tries = 0;
    do {
        lock_instance_res = lock_file(instance_download_binary);
    } while (lock_instance_res == -1 && ++tries < max_recover_tries);

    if (lock_instance_res == 1) {
        got_lock = 1;
        log_message(LOG_DEBUG, "locked! downloading instance..");

        File_lock_update ilu;
        ilu.finished = 0;
        ilu.fsid = fsid;
        ilu.filename = instance_download_binary;
        pthread_t thread;
        pthread_create(&thread, NULL, update_file_lock_thread, (void*) &ilu);

        if (!db_get_instance_binary(instance, instance_download_binary)) {
            log_error(AT, "Could not receive instance binary.");
        }

        if (is_lzma(instance_download_binary)) {
            log_message(LOG_DEBUG, "Extracting instance..");
            string instance_binary_lzma = instance_download_binary + ".lzma";
            rename(instance_download_binary.c_str(), instance_binary_lzma.c_str());
            int res = lzma_extract(instance_binary_lzma, instance_download_binary);
            remove(instance_binary_lzma.c_str());
            if (!res) {
                log_error(AT, "Could not extract %s.", instance_binary_lzma.c_str());
                return 0;
            }
            log_message(LOG_DEBUG, "..done.");
        }
        ilu.finished = 1;

        pthread_join(thread, NULL);
        unlock_file(instance_download_binary);
        log_message(LOG_DEBUG, "..done.");
    } else {
        log_message(LOG_DEBUG, "could not lock instance, locked by other client");
    }
    if (!got_lock) {
        while (file_locked(instance_download_binary)) {
            log_message(LOG_DEBUG, "waiting for instance download from other client: %s", instance_binary.c_str());
            sleep(DOWNLOAD_REFRESH);
        }
    }
    // final check if instance file is there (with waits for NFS)
    int count = 1;
    while (!file_exists(instance_download_binary) && count <= 10) {
        log_message(LOG_DEBUG, "File doesn't exists.. waiting %d / 10", count);
        sleep(1);
        count++;
    }

    if (!check_md5sum(instance_download_binary, instance.md5)) {
        log_message(LOG_DEBUG, "md5 check failed. giving up.");
        return 0;
    }

    if (instance_download_binary != instance_binary) {
        // TODO: check if this is really the case if the paths are not equal
        log_message(LOG_DEBUG, "copying instance binary from download path to base path..");
        if (copy_file(instance_download_binary, instance_binary) == 0) {
            log_error(AT, "Could not copy instance binary. Insufficient rights?");
            return 0;
        }
        if (!check_md5sum(instance_binary, instance.md5)) {
            log_message(LOG_DEBUG, "Final MD5 check before solver uses instance failed.");
            return 0;
        }
    }
    return 1;
}

/**
 * Tries to get the binary of the specified solver. Makes sure that no other client is downloading<br/>
 * this binary on the same file system at the same time.<br/>
 * <br/>
 * On success, the <code>solver_binary</code> contains the file name of the solver and it is<br/>
 * guaranteed that the md5 sum matches the md5 sum in the database.
 * @param solver the solver for which the binary should be downloaded
 * @param solver_binary contains the file name to the binary after download and is set by this function
 * @return value > 0: success
 */
int get_solver_binary(Solver& solver, string& solver_base_path) {
    solver_base_path = solver_path + "/" + solver.md5;
    string solver_download_base_path = solver_download_path + "/" + solver.md5;
    string solver_archive_path = solver_download_base_path + ".zip";
    string solver_extract_path = solver_download_base_path + ".extracting";

    log_message(LOG_DEBUG, "getting solver %s", solver.binaryName.c_str());
    if (file_exists(solver_base_path)) {
        log_message(LOG_DEBUG, "solver binary exists.");
        return 1;
    }
    if (file_exists(solver_download_base_path)) {
        log_message(LOG_DEBUG, "copying solver from download path to base path..");
        if (solver_download_path != solver_base_path) {
            copy_directory(solver_download_base_path, solver_base_path);
        }
        // TODO: use own recursive chmod function
        int exitcode = system((string("chmod -R 777 ") + "\"" + solver_base_path + "\"").c_str());
        if (exitcode == -1) {
            log_error(AT, "Error executing chmod -R 777 command");
            return 0;
        }
        log_message(LOG_DEBUG, ".. done.");
        return 1;
    }
    log_message(LOG_DEBUG, "solver doesn't exist");
    int got_lock = 0;
    log_message(LOG_DEBUG, "trying to lock solver for download");
    int lock_solver_res;
    unsigned int tries = 0;
    do {
        lock_solver_res = lock_file(solver_archive_path);
    } while (lock_solver_res == -1 && ++tries < max_recover_tries);
    
    if (lock_solver_res == 1) {
        log_message(LOG_DEBUG, "locked! downloading solver..");
        File_lock_update slu;
        slu.finished = 0;
        slu.fsid = fsid;
        slu.filename = solver_archive_path;
        pthread_t thread;
        pthread_create(&thread, NULL, update_file_lock_thread, (void*) &slu);
        got_lock = 1;
        if (!db_get_solver_binary(solver, solver_archive_path)) {
            log_error(AT, "Could not receive solver binary archive.");
        }
        bool md5_check = check_md5sum(solver_archive_path, solver.md5);
            if (!create_directory(solver_extract_path)) {
                log_error(AT, "Could not create temporary directory for extraction");
            }
            unsigned char md5[16];
            if (!decompress(solver_archive_path.c_str(), solver_extract_path.c_str(), md5)) {
                log_error(AT, "Error occured when decompressing solver archive");
            } else {
                char md5String[33];
                char* md5StringPtr;
                int i;
                for (i = 0, md5StringPtr = md5String; i < 16; ++i, md5StringPtr += 2)
                    sprintf(md5StringPtr, "%02x", md5[i]);
                md5String[32] = '\0';
                string md5_str(md5String);
                md5_check |= md5_str == solver.md5;
                if (!md5_check) {
                    log_message(LOG_IMPORTANT, "md5 check of solver binary archive %s failed.", solver_archive_path.c_str());
                } else if (!rename(solver_extract_path, solver_download_base_path)) {
                    log_error(AT, "Couldn't rename temporary extraction directory");
                } else {
                    // TODO: use own recursive chmod function
                    int exitcode = system((string("chmod -R 777 ") + "\"" + solver_download_base_path + "\"").c_str());
                    if (exitcode == -1) {
                        log_error(AT, "Error executing chmod -R 777 command");
                        return 0;
                    }
                }
            }


        slu.finished = 1;

        pthread_join(thread, NULL);
        unlock_file(solver_archive_path);
        log_message(LOG_DEBUG, "..done.");
    } else {
        log_message(LOG_DEBUG, "could not lock solver, locked by other client");
    }
    if (!got_lock) {
        while (file_locked(solver_archive_path)) {
            log_message(LOG_DEBUG, "waiting for solver download from other client: %s", solver.binaryName.c_str());
            sleep(DOWNLOAD_REFRESH);
        }

        // final check if the folder is there (with waits for NFS)
        int count = 1;
        while (!file_exists(solver_download_base_path) && count <= 10) {
            log_message(LOG_DEBUG, "File doesn't exists.. waiting %d / 10", count);
            sleep(1);
            count++;
        }
    }
    if (file_exists(solver_download_base_path) && !file_exists(solver_base_path)) {
        log_message(LOG_DEBUG, "copying solver from download path to base path..");
        copy_directory(solver_download_base_path, solver_base_path);
        // TODO: use own recursive chmod function
        int exitcode = system((string("chmod -R 777 ") + "\"" + solver_base_path + "\"").c_str());
        if (exitcode == -1) {
            log_error(AT, "Error executing chmod -R 777 command");
            return 0;
        }

        log_message(LOG_DEBUG, ".. done.");
    }
    return file_exists(solver_base_path);
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
        if (row[2] == NULL)
            param.prefix = "";
        else
            param.prefix = row[2];
        param.hasValue = atoi(row[3]) != 0;
        if (row[4] == NULL)
            param.defaultValue = "";
        else
            param.defaultValue = row[4];
        param.order = atoi(row[5]);
        if (row[6] == NULL)
            param.space = false;
        else
            param.space = atoi(row[6]) != 0;
        if (row[7] == NULL)
            param.attachToPrevious = false;
        else
            param.attachToPrevious = atoi(row[7]) != 0;
        if (row[8] == NULL)
            param.value = "";
        else
            param.value = row[8];
        params.push_back(param);
    }
    mysql_free_result(result);
    return 1;
}

/**
 * Resets a job that was set to running but not actually started
 * back to 'not running'.
 * 
 * @param job_id ID of the job to reset
 * @return 1 on success, 0 on errors
 */
int db_reset_job(int job_id) {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_RESET_JOB, job_id);

    unsigned int tries = 0;
    do {
        if (database_query_update(query) == 1) {
            delete[] query;
            return 1;
        }
    } while (is_recoverable_error() && ++tries < max_recover_tries);

    log_error(AT, "Couldn't execute query to reset job");
    delete[] query;
    return 0;
}

int escape_string_with_limits(MYSQL* con, const char *from, unsigned long max_len, bool limit, int first, int last, char **output) {
    log_message(LOG_DEBUG, "Output limits - first: %d, last %d", first, last);
    // copy output from position 0 (inclusive) to pos_first_end (exclusive)
    unsigned long pos_first_end = max_len;
    // copy output from position pos_last_begin (inclusive) to max_len (exclusive)
    unsigned long pos_last_begin = max_len;
    if (limit) {
        log_message(LOG_DEBUG, "limit output is true");
        if (first < 0 || last < 0) {
            // limit by lines
            first = -first;
            last = -last;
            for (pos_first_end = 0; (pos_first_end < max_len) && (first > 0); pos_first_end++) {
                if (from[pos_first_end] == '\n') {
                    first--;
                }
            }
            if (last > 0 && max_len > 0) {
                for (pos_last_begin = max_len-1; pos_last_begin >= 1 && last >= 0; pos_last_begin--) {
                    if (from[pos_last_begin] == '\n') {
                        last--;
                    }
                }
                if (pos_last_begin == 1) {
                    // might not be that what we wanted, but only max. one line more
                    pos_last_begin = 0;
                } else {
                    pos_last_begin++;
                }
            }
        } else {
            pos_first_end = first;
            if ((long long int)max_len - last < 0) {
                pos_last_begin = 0;
            } else {
                pos_last_begin = max_len - last;
            }
        }
        if (pos_last_begin <= pos_first_end) {
            // no limits
            pos_first_end = max_len;
            pos_last_begin = max_len;
        }
    }
    if (pos_first_end > max_len) {
        pos_first_end = max_len;
    }


    int length = (pos_first_end+max_len-pos_last_begin) * 2 + 1;
    log_message(LOG_DEBUG, "Original size: %d, new size: %d, pos_first_end: %d, pos_last_begin: %d", max_len, (pos_first_end+max_len-pos_last_begin), pos_first_end, pos_last_begin);
    *output = new char[length];
    if (*output == 0) {
        log_error(AT, "Ran out of memory when allocating memory for output data!");
        return -1;
    }
    if (length == 1) {
        *output[0] = '\0';
        return 1;
    }
    if (pos_first_end > 0) {
        mysql_real_escape_string(con, *output, from, pos_first_end);
    }
    if (pos_last_begin < max_len) {
        const char *str = "\n[...]\n";
        mysql_real_escape_string(con, *output+strlen(*output), str, strlen(str));
        //(*output)+(pos_first_end*2-2)
        mysql_real_escape_string(con, *output+strlen(*output), from+pos_last_begin, max_len - pos_last_begin);
    }
    return length;
}

/**
 * Updates a job row with the data from the passed <code>job</code>.
 * BLOBs are escaped using <code>mysql_real_escape</code>.
 * 
 * @param job The job of which the corresponding database row should be updated.
 * @param writeSolverOutput if true: write solver output to database
 * @return 1 on success, 0 on errors
 */
int db_update_job(const Job& job) {
    char* escaped_solver_output;
    char* escaped_launcher_output;
    char* escaped_verifier_output;
    char* escaped_watcher_output;
    int escaped_solver_output_length;
    int escaped_launcher_output_length;
    int escaped_verifier_output_length;
    int escaped_watcher_output_length;
    if ((escaped_solver_output_length = escape_string_with_limits(connection, job.solverOutput, job.solverOutput_length, job.limit_solver_output, job.solver_output_preserve_first, job.solver_output_preserve_last, &escaped_solver_output)) < 1) {
        log_error(AT, "Could not generate escaped solver output string.");
        return 0;
    }
    if ((escaped_launcher_output_length = escape_string_with_limits(connection, job.launcherOutput.c_str(), job.launcherOutput.length(), false, 0, 0, &escaped_launcher_output)) < 1) {
        log_error(AT, "Could not generate escaped solver output string.");
        return 0;
    }
    if ((escaped_verifier_output_length = escape_string_with_limits(connection, job.verifierOutput, job.verifierOutput_length, job.limit_verifier_output, job.verifier_output_preserve_first, job.verifier_output_preserve_last, &escaped_verifier_output)) < 1) {
        log_error(AT, "Could not generate escaped solver output string.");
        return 0;
    }
    if ((escaped_watcher_output_length = escape_string_with_limits(connection, job.watcherOutput.c_str(), job.watcherOutput.length(), job.limit_watcher_output, job.watcher_output_preserve_first, job.watcher_output_preserve_last, &escaped_watcher_output)) < 1) {
        log_error(AT, "Could not generate escaped solver output string.");
        return 0;
    }
    unsigned long total_length = escaped_solver_output_length + escaped_launcher_output_length + escaped_verifier_output_length + escaped_watcher_output_length + 1 + 4096;

    //std::ostringstream costStr;
    //costStr << job.cost;
    char* query_job = new char[total_length];
    int queryLength = snprintf(query_job, total_length, QUERY_UPDATE_JOB, job.status, job.resultCode, job.resultTime, job.wallTime,
            escaped_solver_output, escaped_watcher_output, escaped_launcher_output, escaped_verifier_output,
            job.solverExitCode, job.watcherExitCode, job.verifierExitCode, job.cost, job.idJob, job.idJob);

    int status = mysql_real_query(connection, query_job, queryLength + 1);
    if (status != 0) {
        if (mysql_errno(connection) == CR_SERVER_GONE_ERROR || mysql_errno(connection) == CR_SERVER_LOST || is_recoverable_error() || mysql_errno(connection) == ER_NO_REFERENCED_ROW_2) {
            for (int i = 0; i < opt_wait_jobs_time / WAIT_BETWEEN_RECONNECTS; i++) {
                if (mysql_errno(connection) == ER_NO_REFERENCED_ROW_2) {
                    // foreign key constrained failed (probably invalid resultCode)
                    queryLength = snprintf(query_job, total_length, QUERY_UPDATE_JOB, -6, 0, job.resultTime, job.wallTime,
                            escaped_solver_output, escaped_watcher_output, escaped_launcher_output,
                            escaped_verifier_output, job.solverExitCode, job.watcherExitCode, job.verifierExitCode,
                            costStr.str() == "nan" ? "NULL" : costStr.str().c_str(), job.idJob, job.idJob);
                }
                sleep(WAIT_BETWEEN_RECONNECTS);
                if (mysql_real_query(connection, query_job, queryLength + 1) != 0) {
                    // still doesn't work
                    log_error(
                            AT,
                            "Lost connection to server and couldn't \
                            reconnect when executing job update query: %s",
                            mysql_error(connection));

                } else {
                    // successfully re-issued query
                    log_message(LOG_INFO,
                            "Lost connection but successfully re-established \
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

bool db_fetch_jobs_for_simulation(int grid_queue_id, vector<Job*> &jobs) {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_FETCH_JOBS_SIMULATION, grid_queue_id);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_FETCH_JOBS query");
        // TODO: do something
        delete[] query;
        mysql_free_result(result);
        return false;
    }
    delete[] query;
    if (mysql_num_rows(result) < 1) {
        mysql_free_result(result);
        return false; // job was taken by another client between the 2 queries
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        Job* job = new Job();
        job->idJob = atoi(row[0]);
        job->idSolverConfig = atoi(row[1]);
        job->idExperiment = atoi(row[2]);
        job->idInstance = atoi(row[3]);
        job->run = atoi(row[4]);
        if (row[5] != NULL)
            job->seed = atoi(row[5]); // TODO: not NN column
        job->priority = atoi(row[6]);
        if (row[7] != NULL)
            job->CPUTimeLimit = atoi(row[7]);
        if (row[8] != NULL)
            job->wallClockTimeLimit = atoi(row[8]);
        if (row[9] != NULL)
            job->memoryLimit = atoi(row[9]);
        if (row[10] != NULL)
            job->stackSizeLimit = atoi(row[10]);

        job->wallClockTimeLimit = 10;
        jobs.push_back(job);
    }
    mysql_free_result(result);
    return true;
}

bool db_get_status_code_description(int status_code, string &description) {
    char* query = new char[1024];
    snprintf(query, 1024, QUERY_STATUS_CODE_DESCRIPTION, status_code);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_FETCH_JOBS query");
        // TODO: do something
        delete[] query;
        mysql_free_result(result);
        return false;
    }
    delete[] query;
    if (mysql_num_rows(result) < 1) {
        mysql_free_result(result);
        return false; // job was taken by another client between the 2 queries
    }
    MYSQL_ROW row;
    bool res;
    if ((row = mysql_fetch_row(result))) {
        description = string(row[0]);
        res = true;
    } else {
        res = false;
    }
    return res;
}

