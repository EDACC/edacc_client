/*
 * simulate.cc
 *
 *  Created on: 26.06.2011
 *      Author: simon
 */
#include <vector>
#include <map>
#include <iostream>
#include <sstream>
#include "simulate.h"
#include "log.h"
#include "database.h"

using namespace std;
vector<Job*> jobs;
map<int,int> status_codes;
unsigned int current_job;

int simulate_sign_on(int grid_queue) {
    log_message(LOG_IMPORTANT, "Fetching jobs for grid queue id: %d ..", grid_queue);
    db_fetch_jobs_for_simulation(grid_queue, jobs);
    log_message(LOG_IMPORTANT, ".. got %d jobs.", jobs.size());
    current_job = 0;
    return 1;
}

void simulate_sign_off() {

}

bool simulate_choose_experiment(int, Experiment&) {
    return true;
}

int simulate_db_fetch_job(int, int, int, Job& job) {
    if (current_job >= jobs.size()) {
        return -1;
    }
    job = *(jobs[current_job++]);
    return job.idJob;
}

int simulate_db_update_job(const Job& j) {
    if (j.status != 0)
        status_codes[j.status]++;
    return 1;
}

int simulate_increment_core_count(int, int) {
    return 1;
}

void simulate_exit_client() {
    log_message(LOG_IMPORTANT, "Finished simulation.");
    log_message(LOG_IMPORTANT, "");
    log_message(LOG_IMPORTANT, "Summary:");
    log_message(LOG_IMPORTANT, "--------");
    log_message(LOG_IMPORTANT, "");
    log_message(LOG_IMPORTANT, "status codes:");
    for (map<int,int>::iterator it=status_codes.begin() ; it != status_codes.end(); it++ ) {
        stringstream ss;
        string description;
        if (!db_get_status_code_description((*it).first, description)) {
            description = "WARNING: status code not in db";
        }
        ss << description.c_str() << " (" << (*it).first << "): " << (*it).second;
        log_message(LOG_IMPORTANT, ss.str().c_str());
    }
    exit(0);
}

void initialize_simulation(Methods &methods) {
    log_message(LOG_IMPORTANT, "Initializing the simulation mode..\nExperiments are only simulated, no data is written to the db.");
    methods.sign_on = simulate_sign_on;
    methods.sign_off = simulate_sign_off;
    methods.choose_experiment = simulate_choose_experiment;
    methods.db_fetch_job = simulate_db_fetch_job;
    methods.db_update_job = simulate_db_update_job;
    methods.increment_core_count = simulate_increment_core_count;
}

