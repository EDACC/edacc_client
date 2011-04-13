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
#include <sys/stat.h>
#include <cstring>

#include "host_info.h"
#include "log.h"
#include "database.h"
#include "datastructures.h"
#include "signals.h"
#include "file_routines.h"

using namespace std;

extern int optind;
extern char* optarg;
extern int log_verbosity;
string base_path;
string solver_path;
string instance_path;
string result_path;

// forward declarations
void print_usage();
void read_config(string& hostname, string& username, string& password,
				 string& database, int& port, int& grid_queue_id);
void process_jobs(int grid_queue_id, int client_id);
int sign_on();
int sign_off(int client_id);
bool start_job(int grid_queue_id, int client_id, Worker& worker);
void handle_workers(vector<Worker>& workers, int client_id, bool wait);
int fetch_job(int grid_queue_id, int experiment_id, Job& job);
void signal_handler(int signal);
string get_solver_output_filename(const Job& job);
string get_watcher_output_filename(const Job& job);
string build_watcher_command(const Job& job);
string build_solver_command(const Job& job, const string& solver_binary_filename, 
                            const string& instance_binary_filename,
                            const vector<Parameter>& parameters);
int process_results(Job& job);
void exit_client();


static int client_id = -1;
static string database_name;
static time_t t_started_last_job = time(NULL);
static vector<Worker> workers;

// how long to wait for jobs before exiting
static time_t opt_wait_jobs_time = 10;
// how long to wait between checking for terminated children
static unsigned int opt_check_jobs_interval = 1;

int main(int argc, char* argv[]) {
    // parse command line arguments
	static const struct option long_options[] = {
        { "verbosity", required_argument, 0, 'v' },
        { "logfile", no_argument, 0, 'l' },
        { "wait_time", required_argument, 0, 'w' },
        { "check_interval", required_argument, 0, 'i' },
         0 };

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
        case 'w':
            opt_wait_jobs_time = atoi(optarg);
            break;
        case 'i':
            opt_check_jobs_interval = atoi(optarg);
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
	base_path = ".";

	// read configuration
	string hostname, username, password, database;
	int port = -1, grid_queue_id = -1;
	read_config(hostname, username, password, database, port, grid_queue_id);
    if (hostname == "" || username == "" || database == ""
		|| port == -1 || grid_queue_id == -1) {
		log_error(AT, "Invalid configuration file!");
		return 1;
	}
    database_name = database;

    // set up dirs
	instance_path = base_path + "/instances";
	solver_path = base_path + "/solvers";
	result_path = base_path + "/results";
    if (!create_directories()) {
		log_error(AT, "Couldn't create required folders.");
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
        exit_client();
    }
    int num_worker_slots = grid_queue.numCPUs;
    workers.resize(num_worker_slots, Worker());
    log_message(LOG_DEBUG, "Initialized %d worker slots", num_worker_slots);
    while (true) {
        bool started_job = false;
        for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
            if (it->used == false) {
                started_job |= start_job(grid_queue_id, client_id, *it);
            }
        }
        
        bool any_running_jobs = false;
        for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
            any_running_jobs |= it->used;
        }
        if (!any_running_jobs && time(NULL) - t_started_last_job > opt_wait_jobs_time) {
            // got no jobs since opt_wait_jobs_time seconds and there aren't any jobs running.
            // Exit cleanly.
            log_message(LOG_IMPORTANT, "Didn't start any jobs within the last %d seconds and there "
                                        "are no jobs being processed currently. Exiting.", opt_wait_jobs_time);
            handle_workers(workers, client_id, true);
            exit_client();
        }
        
        handle_workers(workers, client_id, false);
        sleep(opt_check_jobs_interval);
    }
}

bool start_job(int grid_queue_id, int client_id, Worker& worker) {
    log_message(LOG_DEBUG, "Trying to start processing a job");
    // get list of possible experiments (those with the same grid queue
    // the client was started with)
    log_message(LOG_DEBUG, "Fetching list of experiments:");
    vector<Experiment> experiments;
    get_possible_experiments(grid_queue_id, experiments);
    
    if (experiments.empty()) {
        log_message(LOG_DEBUG, "No experiments available");
        return false;
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
        if (diff >= max_diff) {
            max_diff = diff;
            chosen_exp = *it;
        }
    }
    log_message(LOG_DEBUG, "Chose experiment %d - %s with difference %.2f",
                    chosen_exp.idExperiment, chosen_exp.name.c_str(), max_diff);
    
    Job job;
    int job_id = fetch_job(grid_queue_id, chosen_exp.idExperiment, job);
    if (job_id != -1) {
        worker.used = true;
        worker.current_job = job;
        Solver solver;
        Instance instance;
        string instance_binary;
        string solver_binary;

        if (!get_solver(job, solver)) {
        	log_error(AT, "Could not receive solver information.");
        	job.status = -5;
            db_update_job(job);
        	return false;
        }

        if (!get_instance(job, instance)) {
        	log_error(AT, "Could not receive instance information.");
        	job.status = -5;
            db_update_job(job);
        	return false;
        }
        if (!get_instance_binary(instance, instance_binary, 1)) {
        	log_error(AT, "Could not receive instance binary.");
        	job.status = -5;
            db_update_job(job);
        	return false;
        }
        if (!get_solver_binary(solver, solver_binary, 1)) {
        	log_error(AT, "Could not receive solver binary.");
        	job.status = -5;
            db_update_job(job);
        	return false;
        }

        log_message(0, "Solver binary at %s", solver_binary.c_str());
        log_message(0, "Instance binary at %s", instance_binary.c_str());
        
        vector<Parameter> solver_parameters;
        if (get_solver_config_params(job.idSolverConfig, solver_parameters) != 1) {
            log_error(AT, "Could not receive solver config parameters");
            // TODO: do something
        }
        
        string launch_command = build_watcher_command(job);
        launch_command += " ";
        launch_command += build_solver_command(job, solver_binary, instance_binary, solver_parameters);
        log_message(LOG_IMPORTANT, "Launching job with: %s", launch_command.c_str());

        int pid = fork();
        if (pid == 0) { // this is the child
            defer_signals();
            char* command = new char[launch_command.length() + 1];
            strcpy(command, launch_command.c_str());
            char* exec_argv[4] = {"/bin/bash" , "-c", command, NULL};
            if (execve("/bin/bash", exec_argv, NULL) == -1) {
                log_error(AT, "Error in execve()");
                // todo: do something
            }
            // todo: shouldn't happen, do something
        }
        else if (pid > 0) { // fork was successful, this is the parent
            t_started_last_job = time(NULL);
            worker.pid = pid;
            increment_core_count(client_id, chosen_exp.idExperiment);
            return true;
        }
        else {
            log_error(AT, "Couldn't fork");
            // TODO: do something
            exit(1);
        }
    }
    
    return false;
}

string get_watcher_output_filename(const Job& job) {
    ostringstream oss;
    oss << result_path << "/" << database_name << "_" << job.idJob << ".w";
    return oss.str();
}

string get_solver_output_filename(const Job& job) {
    ostringstream oss;
    oss << result_path << "/" << database_name << "_" << job.idJob << ".o";
    return oss.str();
}

/**
 * Builds the watcher command up to the point where the solver binary and
 * parameters should follow.
 * 
 * e.g. "./runsolver --timestamp -w abc.w -o abc.o -C 1000"
 * Notice there's no trailing whitespace!
 */
string build_watcher_command(const Job& job) {
    string watcher_out_file = get_watcher_output_filename(job);
    string solver_out_file = get_solver_output_filename(job);
    ostringstream cmd;
    cmd << "./runsolver --timestamp";
    cmd << " -w " << watcher_out_file;
    cmd << " -o " << solver_out_file;
    
    if (job.CPUTimeLimit != -1) cmd << " -C " << job.CPUTimeLimit;
    if (job.wallClockTimeLimit != -1) cmd << " -W " << job.wallClockTimeLimit;
    if (job.memoryLimit != -1) cmd << " -M " << job.memoryLimit;
    if (job.stackSizeLimit != -1) cmd << " -S " << job.stackSizeLimit;
    return cmd.str();
}

/**
 * Builds the solver launch command.
 * 
 * e.g. "./solvers/TNM -seed 13456 -instance ./instances/in1.cnf -p1 1.2"
 */
string build_solver_command(const Job& job, const string& solver_binary_filename, 
                            const string& instance_binary_filename,
                            const vector<Parameter>& parameters) {
    ostringstream cmd;
    cmd << solver_binary_filename;
    for (vector<Parameter>::const_iterator p = parameters.begin(); p != parameters.end(); ++p) {
        cmd << " ";
        cmd << p->prefix;
        if (p->prefix != "") {
            cmd << " "; // TODO: make this dependant on some parameter flag 
            // that tells us if the value and prefix are separated by a space char or not
        }
        if (p->name == "seed") {
            cmd << job.seed;
        }
        else if (p->name == "instance") {
            cmd << instance_binary_filename;
        }
        else {
            if (p->hasValue) {
                cmd << p->value;
            }
        }
    }
    return cmd.str();
}

int fetch_job(int grid_queue_id, int experiment_id, Job& job) {
    int job_id = db_fetch_job(grid_queue_id, experiment_id, job);
    log_message(LOG_DEBUG, "Trying to fetch job, got %d", job_id);
    return job_id;
}

int find_in_stream(istream &stream, const string tokens) {
	stringstream is(tokens);
	string s1,s2;
	while (1) {
		if (is.eof()) {
			return 1;
		}
		if (stream.eof()) {
			return 0;
		}
		stream >> s1;
		is >> s2;
		if (s1 != s2) {
			is.seekg(0);
			is.clear();
		}
	}
}

/**
 * Process the results of a given job.
 */
int process_results(Job& job) {
    // todo: run solver output through verifier
    // fill job fields with the resulting data
	string watcher_output_filename = get_watcher_output_filename(job);
	string solver_output_filename = get_solver_output_filename(job);
	if (!load_file_string(watcher_output_filename, job.watcherOutput)) {
		log_error(AT, "Could not read watcher output file.");
		return 0;
	}
	if (!load_file_binary(solver_output_filename, &job.solverOutput, &job.solverOutput_length)) {
		log_error(AT, "Could not read solver output file.");
		return 0;
	}
    stringstream ss(job.watcherOutput);
    if (find_in_stream(ss, "CPU time (s):")) {
        ss >> job.resultTime;
    	job.status = 1;
    	log_message(LOG_IMPORTANT, "[Job %d] CPUTime: %f", job.idJob, job.resultTime);
    }
    job.resultCode = 0;

    ss.seekg(0); ss.clear();
    if (find_in_stream(ss, "Maximum CPU time exceeded:")) {
		job.status = 21;
		job.resultCode = -21;
		log_message(LOG_IMPORTANT, "[Job %d] CPU time limit exceeded", job.idJob);
		return 1;
    }
    ss.seekg(0); ss.clear();
    if (find_in_stream(ss, "Child ended because it received signal")) {
    	int signal;
    	ss >> signal;
		job.status = -3;
		job.resultCode = -(300+signal);
		log_message(LOG_IMPORTANT, "[Job %d] Received signal %d", job.idJob, signal);
		return 1;
    }

    if (job.status == 1) {
    	log_message(LOG_IMPORTANT, "[Job %d] Successful!", job.idJob);
    	job.resultCode = 11;
    }

    return 1;
}

void handle_workers(vector<Worker>& workers, int client_id, bool wait) {
    log_message(LOG_DEBUG, "Handling workers");
    for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
        if (it->used) {
            int child_pid = it->pid;
            int proc_stat;
            
            int pid = waitpid(child_pid, &proc_stat, (wait ? 0 : WNOHANG));
            if (pid == child_pid) {
                Job& job = it->current_job;
                if (WIFEXITED(proc_stat)) { // normal watcher exit
                    job.watcherExitCode = WEXITSTATUS(proc_stat);
                    if (process_results(job) != 1) {
                        job.status = -5;
                    }
                    it->used = false;
                    it->pid = 0;
                    decrement_core_count(client_id, it->current_job.idExperiment);
                    db_update_job(job);
                }
                else if (WIFSIGNALED(proc_stat)) { // watcher terminated with a signal
                    job.status = -400 - WTERMSIG(proc_stat);
                    job.resultCode = 0; // unknown result
                    it->used = false;
                    it->pid = 0;
                    decrement_core_count(client_id, it->current_job.idExperiment);
                    db_update_job(job);
                }
            }
        }
    }
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
    port = 3306; // hardcoded for now
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
        else if (id == "port") {
            port = atoi(val.c_str());
        }
	}
	configfile.close();
}

/**
 * Client exit routine that kills any running jobs, cleans up
 * and signs off the client.
 */
void exit_client() {
    defer_signals();
    
    for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
        if (it->used) {
            kill(it->pid, SIGTERM);
            it->current_job.status = -5;
            it->current_job.resultCode = 0;
            db_update_job(it->current_job);
        }
    }

    sign_off(client_id);
    database_close();
    log_close();
    exit(0);
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
        exit_client();
    }
    
    exit_client();
}
