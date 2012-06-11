/*
 * jobserver.cpp
 *
 *  Created on: 05.10.2011
 *      Author: simon
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <cstring>
#include <sstream>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>

#include "jobserver.h"
#include "log.h"
#include "md5sum.h"

static int client_protocol_version = 2;

Jobserver::Jobserver(string hostname, string database, string username, string password, int port) {
    this->connected = false;
    this->fd = -1;
    this->hostname = hostname;
    this->database = database;
    this->username = username;
    this->password = password;
    this->port = port;
}

bool Jobserver::connectToJobserver() {
    if (fd != -1) {
        log_message(LOG_IMPORTANT, "Disconnecting from Job Server caused by previous errors (maybe out of sync or job server shutdown).");
        close(fd);
        fd = -1;
    }
    this->connected = false;
    log_message(LOG_IMPORTANT, "WARNING: Using alternative fetch job id method. This is experimental.");
    log_message(LOG_IMPORTANT, "Connecting to %s:%d", hostname.c_str(), port);
    fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    if (fd < 0) {
        log_error(AT, "Couldn't create socket.");
        return false;
    }
    struct hostent *server = gethostbyname(hostname.c_str());
    if (server == NULL) {
        log_error(AT, "ERROR, no such host");
        return false;
    }
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr, (char *) &serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);
    if (connect(fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        log_error(AT, "Error while connecting.");
        return false;
    }
    int version;
    if (read(fd, &version, 4) != 4) {
        log_message(LOG_IMPORTANT, "Could not read version number. Exiting.");
        return false;
    }
    version = ntohl(version);
    if (version != client_protocol_version) {
        log_message(LOG_IMPORTANT, "Job Server is talking protocol version %d. I'm understanding protocol version %d only. Exiting.", version, client_protocol_version);
        return false;
    }
    int version_nw = htonl(client_protocol_version);
    if (write(fd, &version_nw, 4) != 4) {
        log_message(LOG_IMPORTANT, "Could not send protocol version number. Exiting.");
        return false;
    }
    char magic[13] = "EDACC_CLIENT";
    if (write(fd, magic, 12) != 12) {
        log_message(LOG_IMPORTANT, "Could not send magic number. Exiting.");
        return false;
    }
    int hash_rand;
    if (read(fd, &hash_rand, 4) != 4) {
        log_message(LOG_IMPORTANT, "Could not receive hash number. Exiting.");
        return false;
    }
    hash_rand = ntohl(hash_rand);
    stringstream ss;
    ss << hash_rand << username << password;
    unsigned char md5sum[16];
    md5_buffer(ss.str().c_str(), ss.str().length(), md5sum);

  /*  char md5String[33];
    char* md5StringPtr;
    int i;
    for (i = 0, md5StringPtr = md5String; i < 16; ++i, md5StringPtr += 2)
        sprintf(md5StringPtr, "%02x", md5sum[i]);
    md5String[32] = '\0';
    log_message(LOG_IMPORTANT, "MD5SUM: %s", md5String);*/

    if (write(fd, md5sum, 16) != 16) {
        log_message(LOG_IMPORTANT, "Could not send md5 checksum. Exiting.");
        return false;
    }
    int db_len = database.size();
    int db_len_nw = htonl(db_len);
    if (write(fd, &db_len_nw, 4) != 4) {
        log_message(LOG_IMPORTANT, "Could not send database name length. Exiting.");
        return false;
    }
    if (write(fd, database.c_str(), db_len+1) != db_len+1) {
        log_message(LOG_IMPORTANT, "Could not send database name. Exiting.");
        return false;
    }
    this->connected = true;
    log_message(LOG_IMPORTANT, "connected.");
    return true;
}

bool Jobserver::checkConnection() {
    if (!connected) {
        log_message(LOG_IMPORTANT, "Not connected to jobserver. Waiting 5 seconds..");
        sleep(5);
        connectToJobserver();
    }
    return connected;
}

bool Jobserver::getPossibleExperimentIds(int grid_queue_id, string& ids) {
    log_message(LOG_DEBUG, "Receiving experiment ids from job server..");
    if (!checkConnection()) {
        return false;
    }
    short func_id = htons(0);
    int grid_queue_id_nw = htonl(grid_queue_id);
    int retval;
    retval = write(fd, &func_id, 2);
    if (retval != 2) {
        log_error(AT, "Error while sending function id.");
        connected = false;
        return false;
    }
    retval = write(fd, &grid_queue_id_nw, 4);
    if (retval != 4) {
        log_error(AT, "Error while sending grid queue id.");
        connected = false;
        return false;
    }
    stringstream ss;
    int size;
    retval = read(fd, &size, 4);
    if (retval != 4) {
        log_error(AT, "Error while reading size of experiment id list.");
        connected = false;
        return false;
    }
    size = ntohl(size);
    if (size == 0) {
        log_message(LOG_DEBUG, ".. no experiments available");
        ids = "";
        return true;
    }
    log_message(LOG_DEBUG, ".. size of experiment list is %d.", size);
    for (int i = 0; i < size; i++) {
        int exp_id;
        retval = read(fd, &exp_id, 4);
        if (retval != 4) {
            connected = false;
            return false;
        }
        exp_id = ntohl(exp_id);
        ss << exp_id;
        if (i != size-1) {
            ss << ",";
        }
    }
    ids = ss.str();
    log_message(LOG_DEBUG, "IDs of experiments: %s", ids.c_str());
    return true;
}

bool Jobserver::getJobId(int experiment_id, int &idJob) {
    if (!checkConnection()) {
        return false;
    }
    log_message(LOG_DEBUG, "Trying to receive a job id: sending experiment id %d to job server..", experiment_id);
    short func_id = htons(1);
    int exp_id_nw = htonl(experiment_id);
    int retval;
    retval = write(fd, &func_id, 2);
    if (retval != 2) {
        log_error(AT, "Error while sending function id.");
        connected = false;
        return false;
    }
    retval = write(fd, &exp_id_nw, 4);
    if (retval != 4) {
        log_error(AT, "Error while sending experiment id.");
        connected = false;
        return false;
    }
   // log_message(LOG_DEBUG, "sent. Receiving job id..");
    retval = read(fd, &idJob, 4);
    if (retval != 4) {
        log_error(AT, "Error while reading job id.");
        connected = false;
        return false;
    }
    idJob = ntohl(idJob);
    log_message(LOG_DEBUG, "Received job id: %d", idJob);
    return true;
}
