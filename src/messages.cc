#include <string>
#include <sstream>
#include <pthread.h>
#include <vector>
#include <signal.h>
#include "messages.h"
#include "log.h"
#include "database.h"


using namespace std;

// from client.cc
extern void kill_job(int job_id);
extern void kill_client(int method);

const int MESSAGE_WAIT_TIME = 2;
static MYSQL* connection;
static bool finished;
static pthread_t thread;
static pthread_mutex_t msgs_mutex = PTHREAD_MUTEX_INITIALIZER;
static int client_id;
static vector<string> msgs;

extern int opt_wait_jobs_time;
extern time_t t_started_last_job;

/**
 * Signal handler for SIGINT
 * @param signal
 */
void message_thread_sighandler(int) {
}

/**
 * Checks if there are any messages in the client's database entry.
 * Also clears the message column in the process to indicate that
 * the messages have been received.
 */
void check_message() {
    log_message(LOG_DEBUG, "Checking message..");
    string message;
    //defer_signals();
    int cur_wait_time;
    if (time(NULL) - t_started_last_job > LONG_MAX) {
        cur_wait_time = LONG_MAX;
    } else {
        cur_wait_time = time(NULL) - t_started_last_job;
    }
    if (get_message(client_id, opt_wait_jobs_time, cur_wait_time, message, connection) == 0) {
        //reset_signal_handler();
        return;
    }
    //reset_signal_handler();
    stringstream str(message);
    string line;
    pthread_mutex_lock(&msgs_mutex);
    while (getline(str, line)) {
        log_message(LOG_DEBUG, "Got message: %s", line.c_str());
        msgs.push_back(line);
    }
    pthread_mutex_unlock(&msgs_mutex);
    log_message(LOG_DEBUG, "End of checking message.");
}

/**
 * The message thread. Receives messages and puts them into a queue.
 */
void *message_thread(void*) {
    // initialize signal handler, now sleep can be interrupted
    signal(SIGINT, message_thread_sighandler);

    log_message(LOG_INFO, "Message thread started.");
    if (!get_new_connection(connection)) {
        log_error(AT, "Could not establish database connection.");
        return NULL;
    }
    while (!finished) {
        check_message();
        sleep(MESSAGE_WAIT_TIME);
    }
    mysql_close(connection);
    return NULL;
}

/**
 * Starts the message thread. This method will return immediately after creating the thread.
 */
void start_message_thread(int _client_id) {
    client_id = _client_id;
    finished = false;
    pthread_create( &thread, NULL, message_thread, NULL);
}

/**
 * Stops the message thread. Waits until the message thread did a clean shutdown.
 */
void stop_message_thread() {
    finished = true;
    log_message(LOG_INFO, "Waiting for message thread..");
    // interrupt sleep
    // this results in an ugly error sometimes ..
    // now we wait max. MESSAGE_WAIT_TIME to finish the thread
    //pthread_kill(thread, SIGINT);
    // wait for deinitialization
    pthread_join(thread, NULL);
    log_message(LOG_INFO, "..done.");
}

// declared in client.cc
extern void update_wait_jobs_time(time_t new_wait_time);

/**
 * Should be called by the main loop to process pending messages.
 */
void process_messages() {
    pthread_mutex_lock(&msgs_mutex);
    if (msgs.empty()) {
        pthread_mutex_unlock(&msgs_mutex);
        return;
    }
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
        else if (cmd == "wait_time") {
            time_t time;
            ss >> time;
            if (time != 0)
                update_wait_jobs_time(time);
        }
    }
}
