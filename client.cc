#include <iostream>
#include <cstdlib>
#include <string>
#include <sstream>
#include <unistd.h>
#include <fstream>
#include <vector>
#include <getopt.h>

#include "host_info.h"
#include "log.h"
#include "database.h"
#include "datastructures.h"

using namespace std;

extern int optind;
extern char* optarg;
extern int log_verbosity;

// forward declarations
void print_usage();
void read_config(string& hostname, string& username, string& password,
				 string& database, int& port, int& grid_queue_id);
void process_jobs(int grid_queue_id, int client_id);
int sign_on();
int sign_off(int client_id);
void start_job(int grid_queue_id, int client_id);
void handle_workers();


int main(int argc, char* argv[]) {
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

		stringstream sspid;
		sspid << getpid();
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
	
	int client_id = sign_on();
	
	// run the main client loop
	process_jobs(grid_queue_id, client_id);
	
	sign_off(client_id);
	
	// close database connection and log file
	database_close();
	log_close();
    return 0;
}

int sign_on() {
    log_message(0, "Signing on");
    int client_id = insert_client();
    if (client_id == 0) {
        log_error(AT, "Couldn't sign on. Exiting");
        exit(1);
    }
    log_message(0, "Signed on, got assigned client id %d", client_id);
    
	return client_id;
}

void process_jobs(int grid_queue_id, int client_id) {
    int num_worker_slots = get_num_processors();
    vector<Worker> workers(num_worker_slots, Worker());
    log_message(4, "Initialized %d worker slots", num_worker_slots);
    while (true) {
        for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
            if (it->used == false) {
                start_job(grid_queue_id, client_id);
            }
        }
        handle_workers();

        
        break;
    }
}

void start_job(int grid_queue_id, int client_id) {
    log_message(4, "Trying to start processing a job");
    // get list of possible experiments (those with the same grid queue
    // the client was started with)
    log_message(4, "Fetching list of experiments:");
    vector<Experiment> experiments;
    get_possible_experiments(grid_queue_id, experiments);
    
    for (vector<Experiment>::iterator it = experiments.begin(); it != experiments.end(); ++it) {
        log_message(4, "%d %s %d", it->idExperiment, it->name.c_str(), it->priority);
    }
    
    log_message(4, "Fetching number of CPUs working on each experiment");
    map<int, int> cpu_count_by_experiment;
    get_experiment_cpu_count(cpu_count_by_experiment);
    int sum_cpus = 0;
    for (map<int, int>::iterator it = cpu_count_by_experiment.begin();
            it != cpu_count_by_experiment.end(); ++it) {
        sum_cpus += it->second;
    
    }
    log_message(4, "Total number of CPUs processing experiments currently: %d", sum_cpus);
}

void handle_workers() {
    
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
