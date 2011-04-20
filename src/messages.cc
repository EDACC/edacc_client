#include <string>
#include <sstream>
#include <pthread.h>
#include <vector>
#include "messages.h"
#include "signals.h"
#include "log.h"
#include "database.h"


using namespace std;

// from client.cc
extern void kill_job(int job_id);
extern void kill_client(int method);

const int MESSAGE_WAIT_TIME = 10;

static MYSQL* connection;
static bool finished;
static pthread_t thread;
static pthread_mutex_t msgs_mutex = PTHREAD_MUTEX_INITIALIZER;
static int client_id;
static vector<string> msgs;
/**
 * Checks if there are any messages in the client's database entry
 * and processes them. Also clears the message column in the process to
 * indicate that the messages have been handled.
 */
void check_message() {
    string message;
    defer_signals();
    if (get_message(client_id, message, connection) == 0) {
        reset_signal_handler();
        return;
    }
    reset_signal_handler();
    stringstream str(message);
    string line;
    pthread_mutex_lock(&msgs_mutex);
    while (getline(str, line)) {
        msgs.push_back(line);
    }
    pthread_mutex_unlock(&msgs_mutex);
}

void *message_thread(void* ptr) {
    log_message(LOG_INFO, "Message thread started.");
    if (!get_new_connection(connection)) {
        log_error(AT, "Could not establish database connection.");
        return NULL;
    }
    int time = 0;
    while (!finished) {
        if (time >= MESSAGE_WAIT_TIME) {
            check_message();
            time = 0;
        }
        sleep(1);
        time++;
    }
    mysql_close(connection);
    return NULL;
}


void start_message_thread(int _client_id) {
    client_id = _client_id;
    finished = false;
    pthread_create( &thread, NULL, message_thread, NULL);
}

void stop_message_thread() {
    finished = true;
    log_message(LOG_INFO, "Waiting for message thread..");
    pthread_join(thread, NULL);
    log_message(LOG_INFO, "..done.");
}

/**
 * Should be called by the main loop to process pending messages.
 */
void process_messages() {
    pthread_mutex_lock(&msgs_mutex);
    // copy the vector, we want to unlock the mutex as fast as possible
    vector<string> tmp = msgs;
    msgs.clear();
    pthread_mutex_unlock(&msgs_mutex);

    // process messages
    vector<string>::iterator it;
    for ( it=tmp.begin() ; it < tmp.end(); it++ ) {
        log_message(LOG_DEBUG, "Processing message: \"%s\"", it->c_str());
        istringstream ss(*it);
        string cmd;
        ss >> cmd;

        if (cmd == "kill") {
            int job_id;
            ss >> job_id;
            if (job_id != 0)
                kill_job(job_id);
        }
        else if (cmd == "kill_client") {
            string method;
            ss >> method;
            if (method == "soft") {
                kill_client(0);
            }
            else if (method == "hard") {
                kill_client(1);
            }
        }
    }
}
