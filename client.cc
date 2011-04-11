#include <iostream>
#include <cstdlib>
#include <string>
#include <sstream>
#include <unistd.h>
#include <cmath>
#include <fstream>
#include <vector>
#include <getopt.h>
#include <sys/wait.h>
#include <signal.h>

#include "host_info.h"
#include "log.h"
#include "database.h"
#include "datastructures.h"
#include "signals.h"

using namespace std;

extern int optind;
extern char* optarg;

// forward declarations
void print_usage();
void read_config(string& hostname, string& username, string& password,
				 string& database, int& port, int& grid_queue_id);
void process_jobs(int grid_queue_id, int client_id);
int sign_on();
int sign_off(int client_id);
void start_job(int grid_queue_id, int client_id, Worker& worker);
void handle_workers(vector<Worker>& workers, int client_id);
int fetch_job(int grid_queue_id, int experiment_id, Job& job);
void signal_handler(int signal);

static int client_id = -1;

int main(int argc, char* argv[]) {
    // parse command line arguments
	static const struct option long_options[] = {
        { "verbosity", required_argument, 0, 'v' },
        { "logfile", no_argument, 0, 'l' }, 0 };

    int opt_verbosity = 0;
    bool opt_logfile = false;
    
	while (optind < argc) {
		int index = -1;
		struct option * opt = 0;
		int result = getopt_long(argc, argv, "v:l", long_options,
				&index);
		if (result == -1)
			break; /* end of list */
		switch (result) {
		case 'v':
			opt_verbosity = atoi(optarg);
			break;
		case 'l':
			opt_logfile = true;
			break;
		case 0: /* all parameter that do not */
			/* appear in the optstring */
			opt = (struct option *) &(long_options[index]);
			cout << opt->name << " was specified" << endl;
			if (opt->has_arg == required_argument)
                cout << "Arg: <" << optarg << ">" << endl;
            cout << endl;
			break;
		default:
			cout << "unknown parameter" << endl;
			print_usage();
			return 1;
		}
	}
	
	// read configuration
	string hostname, username, password, database;
	int port = -1, grid_queue_id = -1;
	read_config(hostname, username, password, database, port, grid_queue_id);
    if (hostname == "" || username == "" || database == ""
		|| port == -1 || grid_queue_id == -1) {
		log_error(AT, "Invalid configuration file!");
		return 1;
	}
	
	/* Set up logfile if the command line parameter to do so is set.
	   Otherwise log goes to stdout.
	   We always assume a shared filesystem so we have to use a filename
	   that somehow uses an unique system property.
	   If we can't get the hostname or ip address, messages from two or 
	   more clients end up in the same logfile. */
	if (opt_logfile) {
        // gather system info
        string syshostname = get_hostname();
        string ipaddress = get_ip_address(false);
        if (ipaddress == "") ipaddress = get_ip_address(true);
        
		string log_filename = syshostname + "_" + ipaddress +
							  "_edacc_client.log";
		log_init(log_filename, opt_verbosity);
	} else {
		log_verbosity = opt_verbosity;
	}
	
	// connect to database
	if (!database_connect(hostname, database, username, password, port)) {
		log_error(AT, "Couldn't establish database connection.");
		return 1;
	}
	
	client_id = sign_on();
    
    // set up signal handler
    set_signal_handler(&signal_handler);
	
	// run the main client loop
	process_jobs(grid_queue_id, client_id);
	
	sign_off(client_id);
	
	// close database connection and log file
	database_close();
	log_close();
    return 0;
}

int sign_on() {
    log_message(LOG_INFO, "Signing on");
    int client_id = insert_client();
    if (client_id == 0) {
        log_error(AT, "Couldn't sign on. Exiting");
        exit(1);
    }
    log_message(LOG_INFO, "Signed on, got assigned client id %d", client_id);
    
	return client_id;
}

void process_jobs(int grid_queue_id, int client_id) {
    GridQueue grid_queue;
    if (get_grid_queue_info(grid_queue_id, grid_queue) != 1) {
        log_error(AT, "Couldn't retrieve grid information");
        sign_off(client_id);
        exit(1);
    }
    int num_worker_slots = grid_queue.numCPUs;
    vector<Worker> workers(num_worker_slots, Worker());
    log_message(LOG_DEBUG, "Initialized %d worker slots", num_worker_slots);
    while (true) {
        for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
            if (it->used == false) {
                start_job(grid_queue_id, client_id, *it);
            }
        }
        handle_workers(workers, client_id);
        // TODO: check client messages
        sleep(1);
    }
}

void start_job(int grid_queue_id, int client_id, Worker& worker) {
    log_message(LOG_DEBUG, "Trying to start processing a job");
    // get list of possible experiments (those with the same grid queue
    // the client was started with)
    log_message(LOG_DEBUG, "Fetching list of experiments:");
    vector<Experiment> experiments;
    get_possible_experiments(grid_queue_id, experiments);
    
    if (experiments.empty()) {
        log_message(LOG_DEBUG, "No experiments available");
        return;
    }
    
    for (vector<Experiment>::iterator it = experiments.begin(); it != experiments.end(); ++it) {
        log_message(LOG_DEBUG, "%d %s %d", it->idExperiment, it->name.c_str(), it->priority);
    }
    
    log_message(LOG_DEBUG, "Fetching number of CPUs working on each experiment");
    map<int, int> cpu_count_by_experiment;
    get_experiment_cpu_count(cpu_count_by_experiment);
    int sum_cpus = 0;
    int priority_sum = 0;
    for (vector<Experiment>::iterator it = experiments.begin(); it != experiments.end(); ++it) {
        if (cpu_count_by_experiment.find((*it).idExperiment) != cpu_count_by_experiment.end()) {
            sum_cpus += cpu_count_by_experiment[it->idExperiment];
        }
        priority_sum += it->priority;
    }
    log_message(LOG_DEBUG, "Total number of CPUs processing possible experiments currently: %d", sum_cpus);
    
    log_message(LOG_DEBUG, "Choosing experiment, priority sum: %d, total CPUs: %d", priority_sum, sum_cpus);
    Experiment chosen_exp;
    float max_diff = 0.0f;
    // find experiment with maximum abs(exp_prio / priority_sum - exp_cpus / sum_cpus)
    // i.e. maximum difference between target priority and current ratio of assigned CPUs
    for (vector<Experiment>::iterator it = experiments.begin(); it != experiments.end(); ++it) {
        float diff;
        if (priority_sum == 0 && sum_cpus == 0) {
            // all priorities 0, no CPUs used
            diff = 0.0f;
        }
        else if (priority_sum == 0) {
            // all experiments have priority 0, try to achieve uniform CPU distribution
            diff = cpu_count_by_experiment[it->idExperiment] / (float)sum_cpus;
        }
        else if (sum_cpus == 0) {
            // no clients processing jobs yet, choose the experiment with highest priority ratio
            diff = it->priority / (float)priority_sum;
        }
        else {
            diff = fabs(it->priority / (float)priority_sum - cpu_count_by_experiment[it->idExperiment] / (float)sum_cpus);
        }
        log_message(LOG_DEBUG, "Experiment %d - %s, prio: %d, CPU count: %d", 
                        it->idExperiment, it->name.c_str(), it->priority, cpu_count_by_experiment[it->idExperiment]);
        if (diff > max_diff) {
            max_diff = diff;
            chosen_exp = *it;
        }
    }
    log_message(LOG_DEBUG, "Chose experiment %d - %s with difference %.2f",
                    chosen_exp.idExperiment, chosen_exp.name.c_str(), max_diff);
    
    Job job;
    int job_id = fetch_job(grid_queue_id, chosen_exp.idExperiment, job);
    if (job_id != -1) {
        // TODO: start job, set worker's pid and used to true
        // increment core count in Experiment_has_Client table
        // some dummy code:
        worker.used = true;
        worker.current_job = job;
        int pid = fork();
        if (pid == 0) {
            defer_signals();
            sleep(10);
            exit(11);
        }
        else {
            worker.pid = pid;
            increment_core_count(client_id, chosen_exp.idExperiment);
        }
        // end dummy code
    }
    else {
        // Got no job, got no jobs for a while?
        // yes -> wait handle running workers & sign off & exit
        // no -> wait x seconds, return & retry
        sign_off(client_id);
        exit(0);
    }
}

int fetch_job(int grid_queue_id, int experiment_id, Job& job) {
    int job_id = db_fetch_job(grid_queue_id, experiment_id, job);
    log_message(LOG_DEBUG, "Trying to fetch job, got %d", job_id);
    return job_id;
}

void handle_workers(vector<Worker>& workers, int client_id) {
    log_message(LOG_DEBUG, "Handling workers");
    // TODO: loop over processing workers:
    // waitpid(worker->pid, (stat_loc*) &stats, WNOHANG)
    // -> returns immediately if no status available
    // if child finished, handle results etc.
    // decrement core count in Experiment_has_Client table
    
    // dummy code
    for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
        if (it->used) {
            int child_pid = it->pid;
            int proc_stat;
            int pid = waitpid(child_pid, &proc_stat, WNOHANG);
            if (pid == child_pid) {
                if (WIFEXITED(proc_stat)) {
                    cout << child_pid << " exited" << endl;
                    log_message(LOG_DEBUG, "Exit code: %d", WEXITSTATUS(proc_stat));
                    it->used = false;
                    it->pid = 0;
                    cout << "decrement core count " << client_id << " exp: " << it->current_job.idExperiment << endl;
                    decrement_core_count(client_id, it->current_job.idExperiment);
                }
                else if (WIFSIGNALED(proc_stat)) {
                    
                }
            }
        }
    }// end dummy code
}

int sign_off(int client_id) {
	delete_client(client_id);
    log_message(0, "Signed off");
	return 1;
}

/**
 * Reads the configuration file './config' that consists of lines of
 * key-value pairs separated by an '=' character.
 */
void read_config(string& hostname, string& username, string& password,
				 string& database, int& port, int& grid_queue_id) {
	ifstream configfile("./config");
	if (!configfile.is_open()) {
		log_message(0, "Couldn't open config file. Make sure 'config' \
						exists in the client's working directory.");
		return;
	}
	string line;
	while (getline(configfile, line)) {
		istringstream iss(line);
		string id, val, eq;
		iss >> id >> eq >> val;
		if (id == "host") {
			hostname = val;
		}
		else if (id == "username") {
			username = val;
		}
		else if (id == "password") {
			password = val;
		}
		else if (id == "database") {
			database = val;
		}
		else if (id == "gridqueue") {
			grid_queue_id = atoi(val.c_str());
		}
	}
	port = 3306; // hardcoded for now
	configfile.close();
}

/**
 * Print accepted command line parameters and other hints
 */
void print_usage() {
    cout << "EDACC Client\n" << endl;
}

void signal_handler(int signal) {
    log_message(LOG_DEBUG, "Caught signal %d", signal);
    if (signal == SIGINT) {
        sign_off(client_id);
        exit(0);
    }
}
