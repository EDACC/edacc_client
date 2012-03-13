/*
 * database_fs_locking.cc
 *
 *  Created on: 13.03.2012
 *      Author: simon
 */

#include <string>
#include <vector>
#include <mysql/mysqld_error.h>
#include "database.h"
#include "database_fs_locking.h"

#include "log.h"

// the fsid, from database.cc
extern int fsid;
extern MYSQL* connection;

/**
 * Tries to update the file lock.<br/>
 * @param instance the instance for which the lock should be updated
 * @return value != 0: success
 */
int update_file_lock(string &filename) {
    char *query = new char[4096];
    snprintf(query, 1024, QUERY_UPDATE_FILE_LOCK, filename.c_str(), fsid);

    unsigned int tries = 0;
    do {
        if (database_query_update(query) == 1) {
            delete[] query;
            return mysql_affected_rows(connection) == 1;
        }
    } while (is_recoverable_error() && ++tries < max_recover_tries);

    log_error(AT, "Couldn't execute QUERY_UPDATE_FILE_LOCK query");
    // TODO: do something
    delete[] query;
    return 0;
}


/**
 * Locks a file.<br/>
 * <br/>
 * On success it is guaranteed that this file was locked.
 * @param filename the name of the file which should be locked
 * @return -1 when the operation should be tried again, 0 on errors, 1 on success
 */
int lock_file(string &filename) {
    mysql_autocommit(connection, 0);

    // this query locks the entry with (filename, fsid) if existent
    // this is needed to determine if the the client which locked this file is dead
    //  => only one client should check this and update the lock
    char *query = new char[4096];
    snprintf(query, 1024, QUERY_CHECK_FILE_LOCK, filename.c_str(), fsid);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_CHECK_FILE_LOCK query");
        delete[] query;
        mysql_rollback(connection);
        mysql_autocommit(connection, 1);
        return 0;
    }
    delete[] query;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == NULL) {
        // file is currently not locked by another client
        // try to create a lock
        mysql_free_result(result);
        query = new char[1024];
        snprintf(query, 1024, QUERY_LOCK_FILE, filename.c_str(), fsid);
        if (database_query_update(query) == 0) {
            // ER_DUP_ENTRY is ok -> was locked by another client
            if (mysql_errno(connection) != ER_DUP_ENTRY) {
                log_error(AT, "Couldn't execute QUERY_LOCK_FILE query");
            }
            mysql_rollback(connection);
            mysql_autocommit(connection, 1);
            delete[] query;
            return -1;
        }
        mysql_commit(connection);
        mysql_autocommit(connection, 1);
        delete[] query;
        // success
        return 1;
    } else if (atoi(row[0]) > DOWNLOAD_TIMEOUT) {
        // file was locked by another client but DOWNLOAD_TIMEOUT reached
        // try to update the file lock => steal lock from dead client
        // might fail if another client was in the same situation (before the row lock) and faster
        mysql_free_result(result);
        int res = update_file_lock(filename);
        mysql_commit(connection);
        mysql_autocommit(connection, 1);
        return res;
    }
    mysql_free_result(result);
    mysql_commit(connection);
    mysql_autocommit(connection, 1);
    return 0;
}

/**
 * Removes the file lock.
 * @param filename the name of the file for which the lock should be removed
 * @return value != 0: success
 */
int unlock_file(string& filename) {
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_UNLOCK_FILE, filename.c_str(), fsid);

    unsigned int tries = 0;
    do {
        if (database_query_update(query) == 1) {
            delete[] query;
            return 1;
        }
    } while (is_recoverable_error() && ++tries < max_recover_tries);

    log_error(AT, "Couldn't execute QUERY_UNLOCK_FILE query");
    delete[] query;
    return 0;
}

/**
 * Checks if the specified file with the file system id is currently locked by any client.
 * @param filename the filename to be checked
 * @return value != 0: file is locked
 */
int file_locked(string& filename) {
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_CHECK_FILE_LOCK, filename.c_str(), fsid);
    MYSQL_RES* result;
    if (database_query_select(query, result) == 0) {
        log_error(AT, "Couldn't execute QUERY_CHECK_FILE_LOCK query");
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
 * Updates the file lock until finished is set to true in the <code>File_lock_update</code> data structure.<br/>
 * <br/>
 * Will not delete the file lock.
 * @param ptr pointer to <code>File_lock_update</code> data structure.
 */
void *update_file_lock_thread(void* ptr) {
    File_lock_update* ilu = (File_lock_update*) ptr;

    // create connection
    MYSQL* con = NULL;
    if (!get_new_connection(con)) {
        log_error(AT, "[update_instance_lock_thread] Database connection attempt failed: %s", mysql_error(con));
        return NULL;
    }
    // prepare query
    char *query = new char[1024];
    snprintf(query, 1024, QUERY_UPDATE_FILE_LOCK, ilu->filename.c_str(), ilu->fsid);

    int qtime = 0;
    while (!ilu->finished) {
        if (qtime <= 0) {
            // update lastReport entry for this instance
            if (!database_query_update(query, con)) {
                log_error(AT, "[update_file_lock_thread] Couldn't execute QUERY_UPDATE_FILE_LOCK query for binary %s.", ilu->filename.c_str());
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
    log_message(LOG_DEBUG, "[update_file_lock_thread] Closed database connection");
    return NULL;
}
