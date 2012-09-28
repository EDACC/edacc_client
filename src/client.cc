#include <iostream>
#include <iomanip>
#include <cstdlib>
#include <string>
#include <sstream>
#include <unistd.h>
#include <cmath>
#include <fstream>
#include <ctime>
#include <vector>
#include <algorithm>
#include <getopt.h>
#include <sys/wait.h>
#include <signal.h>
#include <sys/stat.h>
#include <cstring>
#ifdef use_hwloc
#include <hwloc.h>
#endif

#include "host_info.h"
#include "log.h"
#include "database.h"
#include "datastructures.h"
#include "signals.h"
#include "file_routines.h"
#include "messages.h"
#include "process.h"
#include "simulate.h"
#include "jobserver.h"

using namespace std;

extern int optind;
extern char* optarg;
extern int log_verbosity;
string base_path;
string download_path;
string solver_path;
string instance_path;
string result_path;
string verifier_path;
string cost_binary_path;
string solver_download_path;
string instance_download_path;
string verifier_download_path;
string cost_binary_download_path;

// forward declarations
void print_usage();
void read_config(string& hostname, string& username, string& password,
				 string& database, int& port, int& grid_queue_id,
				 string& jobserver_hostname, int& jobserver_port,
				 string& sandbox_command);
void process_jobs(int grid_queue_id);
int sign_on(int grid_queue_id);
void sign_off();
void initialize_workers(GridQueue &grid_queue);
bool start_job(int grid_queue_id, Worker& worker);
int handle_workers(vector<Worker>& workers);
void signal_handler(int signal);
string get_solver_output_filename(const Job& job);
string get_watcher_output_filename(const Job& job);
string build_watcher_command(const Job& job);
string build_solver_command(const Job& job, const Solver& solver, const string& solver_base_path, 
                            const string& instance_binary_filename, const string& tempfiles_path,
                            const vector<Parameter>& parameters);
string build_verifier_command(const Verifier& verifier, const string& verifier_base_path,
							  const string& output_solver, const string& instance, const string& output_watcher,
							  const string& output_launcher);
string build_cost_command(const Job& job, const CostBinary& cost_binary, const string& cost_binary_base_path, const string& output_solver, const string& instance);
int process_results(Job& job);
void exit_client(int exitcode, bool wait=false);
string trim_whitespace(const string& str);
bool choose_experiment(int grid_queue_id, Experiment &chosen_exp);
int find_in_stream(istream &stream, const string tokens);
string str_lower(const string& str);
bool parse_watcher_line(istream &stream, const string prefix, float& value);

static int client_id = -1;

static string database_name;
time_t t_started_last_job = time(NULL);
static vector<Worker> workers;
static Job downloading_job;
static HostInfo host_info; // filled onced on sign on
static string sandbox_command;

static Methods methods;

// how long to wait for jobs before exiting
time_t opt_wait_jobs_time = 10;
// how long to wait between checking for terminated children in ms
static unsigned int opt_check_jobs_interval = 20;
// whether to keep solver and watcher output after processing or to delete them
static bool opt_keep_output = false;
// path where the log file should be written
static string opt_log_path;
// base path if specified as command line argument
static string opt_base_path;
// path where files should be downloaded
static string opt_download_path;
// the estimated walltime for the client
static string opt_walltime;
// path to config file
static string opt_config = "./config";
// whether to exit if the client runs on a system with a different CPU than it is
// specified in the grid queue
static bool opt_run_on_inhomogenous_hosts = false;
// whether we only simulate the experiments associated with the grid queue
static bool simulate = false;

// upper limit for check for jobs interval increase in ms if the client didn't get a job despite idle workers
const unsigned int CHECK_JOBS_INTERVAL_UPPER_LIMIT = 10000;

// declared in database.cc
extern Jobserver* jobserver;

char** environp;

#define COMPILATION_TIME "Compiled at "__DATE__" "__TIME__

string tempfiles_base_path = "/tmp/solver_tempfiles";

template <typename T>
T max_(const T& a, const T& b) { return a > b ? a : b; }
template <typename T>
T min_(const T& a, const T& b) { return a < b ? a : b; }

int main(int argc, char* argv[], char **envp) {
    environp = envp;
    if (argc > 1 && string(argv[1]) == "--help") {
        print_usage();
        return 0;
    }

    // parse command line arguments
	static const struct option long_options[] = {
        { "verbosity", required_argument, 0, 'v' },
        { "logfile", no_argument, 0, 'l' },
        { "wait_time", required_argument, 0, 'w' },
        { "check_interval", required_argument, 0, 'i' },
        { "keep_output", no_argument, 0, 'k' },
        { "base_path", no_argument, 0, 'b' },
        { "log_path", required_argument, 0, 'p'},
        { "download_path", required_argument, 0, 'd'},
        { "run_on_inhomogenous_hosts", no_argument, 0, 'h' },
        { "simulate", no_argument, 0, 's'},
        { "walltime", required_argument, 0, 't'},
        { "config", required_argument, 0, 'c' },
        {0,0,0,0} };

	opt_log_path = ".";
    int opt_verbosity = 0;
    bool opt_logfile = false;
    
	while (optind < argc) {
		int index = -1;
		struct option * opt = 0;
		int result = getopt_long(argc, argv, "c:v:lw:i:kb:hsp:d:t:", long_options,
				&index);
		if (result == -1)
			break; /* end of list */
		switch (result) {
        case 'c':
            opt_config = string(optarg);
            break;
		case 'v':
			opt_verbosity = atoi(optarg);
			break;
		case 'p':
		    opt_log_path = string(optarg);
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
        case 'k':
            opt_keep_output = true;
            break;
        case 'b':
            opt_base_path = string(optarg);
            break;
        case 'd':
            opt_download_path = string(optarg);
            break;
        case 'h':
            opt_run_on_inhomogenous_hosts = true;
            break;
        case 's':
            simulate = true;
            break;
        case 't':
            opt_walltime = string(optarg);
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
    if (opt_base_path != "") base_path = opt_base_path;
    if (opt_download_path != "") {
        download_path = opt_download_path;
    } else {
        download_path = base_path;
    }
    base_path = absolute_path(base_path);
    download_path = absolute_path(download_path);

	// read configuration
	string hostname, username, password, database, jobserver_hostname;
	int port = -1, grid_queue_id = -1, jobserver_port = 3307;
	read_config(hostname, username, password, database, port, grid_queue_id, jobserver_hostname, jobserver_port, sandbox_command);
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
	verifier_path = base_path + "/verifiers";
	cost_binary_path = base_path + "/cost_binaries";
	instance_download_path = download_path + "/instances";
	solver_download_path = download_path + "/solvers";
	verifier_download_path = download_path + "/verifiers";
	cost_binary_download_path = download_path + "/cost_binaries";
    if (!(create_directory(instance_path)
            && create_directory(solver_path)
            && create_directory(result_path)
            && create_directory(verifier_path)
            && create_directory(tempfiles_base_path)
            && create_directory(cost_binary_path))) {
		log_error(AT, "Couldn't create required folders.");
		return 1;
	}
	
	/** Set up a log file if the command line parameter to do so is set.
	   Otherwise log goes to stdout.
	   We always assume a shared filesystem so we have to use a filename
	   that somehow uses an unique system property.
	   If we can't get the hostname or ip address, messages from two or 
	   more clients may end up in the same logfile if the process ids
	   happen to be the same */
	if (opt_logfile) {
        // gather system info
        string syshostname = get_hostname();
        string ipaddress = get_ip_address(false);
        if (ipaddress == "") ipaddress = get_ip_address(true);
        ostringstream iss;
        iss << getpid();
        
		string log_filename = opt_log_path + "/" + syshostname + "_" + ipaddress + "_" + iss.str() +
							  "_edacc_client.log";
		if (log_init(log_filename, opt_verbosity) == 0) {
            log_error(AT, "Couldn't initialize logfile, logging to stdout");
        }
	} else {
		log_verbosity = opt_verbosity;
	}
	
	log_message(LOG_IMPORTANT, COMPILATION_TIME);

	// connect to database
	bool connected = false;
	for (int count = 1; count <= 10; count ++) {
	    log_message(LOG_IMPORTANT, "Trying to connect to database. Try %d / 10.", count);
	    if (database_connect(hostname, database, username, password, port)) {
	        connected = true;
	        break;
	    }
	    sleep(2);
	}
	if (!connected) {
	    log_error(AT, "Couldn't establish database connection.");
	    return 1;
	} else {
	    log_message(LOG_IMPORTANT, "Connected.");
	}

	int version = get_current_model_version();
	if (version == -1) {
	    log_error(AT, "Couldn't determine current model version. Exiting");
	    return 1;
	} else if (version < MIN_MODEL_VERSION) {
	    log_message(LOG_IMPORTANT, "DB model version is %d, clients expects model version %d or higher. Exiting.", version, MIN_MODEL_VERSION);
	    return 1;
	} else {
	    log_message(LOG_IMPORTANT, "DB model version is %d.", version);
	}

	if (jobserver_hostname != "") {
        // use alternative fetch job id method
	    jobserver = new Jobserver(jobserver_hostname, database, username, password, jobserver_port);
	    if (!jobserver->connectToJobserver()) {
	        log_message(LOG_IMPORTANT, "Could not connect to jobserver. Exiting.");
	        return 1;
	    }
    } else {
        // don't use alternative fetch job id method
        jobserver = NULL;
    }

	log_message(LOG_INFO, "Gathering host information");
    host_info.num_cores = get_num_physical_cpus();
    host_info.num_threads = get_num_processors();
    host_info.hyperthreading = has_hyperthreading();
    host_info.turboboost = has_turboboost();
    host_info.cpu_model = get_cpu_model();
    host_info.cache_size = get_cache_size();
    host_info.cpu_flags = get_cpu_flags();
    host_info.memory = get_system_memory();
    host_info.free_memory = get_free_system_memory();
    host_info.cpuinfo = get_cpuinfo();
    host_info.meminfo = get_meminfo();
	
    if (simulate) {
        opt_wait_jobs_time = 0;
        initialize_simulation(methods);
    } else {
        methods.sign_on = sign_on;
        methods.sign_off = sign_off;
        methods.choose_experiment = choose_experiment;
        methods.db_fetch_job = db_fetch_job;
        methods.db_update_job = db_update_job;
        methods.increment_core_count = increment_core_count;
    }
	client_id = methods.sign_on(grid_queue_id);

    // set up signal handler
    set_signal_handler(&signal_handler);
	
    if (!simulate)
        start_message_thread(client_id);

	// run the main client loop
	process_jobs(grid_queue_id);
	
	methods.sign_off();
	
	// close database connection and log file
	database_close();
	log_close();
    return 0;
}



/**
 * Sign on to the database.
 * Inserts a new row into the Client table and returns the auto-incremented
 * ID of it.
 * @return The auto-incremented id of the inserted client row
 */
int sign_on(int grid_queue_id) {
    log_message(LOG_INFO, "Signing on");
    int client_id = insert_client(host_info, grid_queue_id, opt_wait_jobs_time, opt_walltime);
    if (client_id == 0) {
        log_error(AT, "Couldn't sign on. Exiting");
        exit(1);
    }
    log_message(LOG_INFO, "Signed on, got assigned client id %d", client_id);
    if (fill_grid_queue_info(host_info, grid_queue_id) != 1) {
        log_message(LOG_IMPORTANT, "Couldn't fill gridQueue row with host info");
    }
    
	return client_id;
}

/**
 * Kills the worker child process that is processing the job with the given
 * ID by sending SIGTERM to it. The job status is set to 'terminated by user'
 * and the worker is made ready to process other jobs again.
 * @param job_id The id of job that should be stopped.
 */
void kill_job(int job_id) {
    if (job_id == -1) {
        log_message(LOG_IMPORTANT, "Killing all jobs.");
    }
    for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
        if (it->used && (job_id == -1 || it->current_job.idJob == job_id)) {
            log_message(LOG_IMPORTANT, "Killing job with id %d.", it->current_job.idJob);
            kill_process(it->pid, 3);
            int proc_stat;
            waitpid(it->pid, &proc_stat, 0);
            
            string watcher_output_filename = get_watcher_output_filename(it->current_job);
            
            log_message(LOG_DEBUG, "Loading watcher output");
            if (!load_file_string(watcher_output_filename, it->current_job.watcherOutput)) {
                log_error(AT, "Could not read watcher output file.");
            }
            log_message(LOG_DEBUG, "Starting to process results");

            stringstream ss(it->current_job.watcherOutput);
            float cputime;
            if (parse_watcher_line(ss, "CPU time (s):", cputime)) {
                it->current_job.resultTime = cputime;
                log_message(LOG_IMPORTANT, "[Job %d] CPUTime: %f", 
                    it->current_job.idJob, it->current_job.resultTime);
            }
            ss.clear(); ss.seekg(0);
            float realtime;
            if (parse_watcher_line(ss, "Real time (s):", realtime)) {
            	it->current_job.wallTime = realtime;
            	log_message(LOG_IMPORTANT, "[Job %d] wall time: %f", it->current_job.idJob, it->current_job.wallTime);
            }

            // TODO: cost binary
    
            it->current_job.launcherOutput = get_log_tail();
            it->current_job.status = 20;
            it->current_job.resultCode = 0;
            defer_signals();
            methods.db_update_job(it->current_job);
            decrement_core_count(client_id, it->current_job.idExperiment);
            reset_signal_handler();
            it->used = false;
            it->pid = 0;
            if (job_id != -1) {
                break;
            }
        }
    }
}

void kill_client(int method) {
    if (method == 0) {
        log_message(LOG_IMPORTANT, "Received soft kill command, waiting for running jobs to finish "
                                   "and exiting after");
        handle_workers(workers);
        exit_client(0, true);
    }
    else if (method == 1) {
        log_message(LOG_IMPORTANT, "Received hard kill command. Exiting immediately.");
        exit_client(0);
    }
}

#ifdef use_hwloc
static void allocate_pus(hwloc_topology_t topology, hwloc_obj_t obj, int depth, set<int> *pu_ids, int num_pus, unsigned int pus_per_set, unsigned int pus_to_allocate) {
    if (pus_to_allocate == 0)
        return;
    if (hwloc_compare_types(obj->type, HWLOC_OBJ_PU) == 0) {
        for (int i = 0; i < num_pus; i++) {
            if (pu_ids[i].size() < pus_per_set) {
                pu_ids[i].insert(obj->os_index);
                break;
            }
        }
    }
    unsigned int num_nodes = obj->arity;
    if (num_nodes == 0) {
        return;
    }
    int num = pus_to_allocate / num_nodes;
    for (unsigned int i = 0; i < num_nodes; i++) {
        int t = num;
        if (i == 0) {
            t += pus_to_allocate % num_nodes;
        }
        allocate_pus(topology, obj->children[i], depth+1, pu_ids, num_pus, pus_per_set, t);
    }
}
#endif

void initialize_workers(GridQueue &grid_queue) {
    int cpus_per_worker = grid_queue.numCPUsPerJob;

    if (grid_queue.numCPUs % cpus_per_worker != 0) {
        log_message(LOG_IMPORTANT, "WARNING: Number of CPUs per worker is not a multiple of number of CPUs for this grid queue.");
    }
    workers.resize(grid_queue.numCPUs / cpus_per_worker, Worker());
    log_message(LOG_IMPORTANT, "Initializing %d workers.", workers.size());

#ifdef use_hwloc
    log_message(LOG_IMPORTANT, "INFORMATION: Using hwloc to determine hardware topology.");

    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    hwloc_topology_load(topology);

    int num_pu = 0;
    int num_cores = 0;
    int num_sockets = 0;
    int depth = hwloc_get_type_depth(topology, HWLOC_OBJ_PU);
    if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
        num_pu = -1;
    } else {
        num_pu = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
    }

    depth = hwloc_get_type_depth(topology, HWLOC_OBJ_CORE);
    if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
        num_cores = -1;
    } else {
        num_cores = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_CORE);
    }

    depth = hwloc_get_type_depth(topology, HWLOC_OBJ_SOCKET);
    if (depth == HWLOC_TYPE_DEPTH_UNKNOWN) {
        num_sockets = -1;
    } else {
        num_sockets = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_SOCKET);
    }

    if (num_pu == -1 || num_cores == -1 || num_sockets == -1) {
        log_message(LOG_IMPORTANT, "WARNING: Could not determine hardware topology. Binding solvers to processing units is not possible.");
    } else {
        log_message(LOG_IMPORTANT, "Found %d socket(s).", num_sockets);
        log_message(LOG_IMPORTANT, "Found %d core(s).", num_cores);
        log_message(LOG_IMPORTANT, "Found %d processing unit(s).", num_pu);
        set<int> cpu_ids[num_pu];
        if (num_pu < grid_queue.numCPUs) {
            log_message(LOG_IMPORTANT, "Number of processing units is less than number of cores specified for this grid queue. Binding solvers to processing units is not possible.");
        } else {
            allocate_pus(topology, hwloc_get_root_obj(topology), 0, cpu_ids, num_pu, cpus_per_worker, grid_queue.numCPUs);
            int current = 0;
            // initialize core ids
            for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
                stringstream s;
                for (set<int>::iterator it2 = cpu_ids[current].begin(); it2 != cpu_ids[current].end(); it2++) {
                    s << *it2;
                    (*it).core_ids.insert(*it2);
                    if (++it2 != cpu_ids[current].end()) {
                        s << ',';
                    }
                    it2--;
                }
                log_message(LOG_IMPORTANT, "Worker %d: PU(s)#%s", current, s.str().c_str());
                current++;
            }
        }
    }
#else
    log_message(LOG_IMPORTANT, "INFORMATION: Not using hwloc. Cores will be allocated by operating system.");
#endif
}

/**
 * This function contains the main processing loop.
 * After fetching the grid queue information from the database
 * numCPUs worker slots are initialized. In a loop then following happens:
 * 1. If there are any unused worker slots, try to find a job for each of them.
 * 2. If the client didn't start processing any jobs since opt_wait_jobs_time
 *    seconds and there aren't any jobs running, it exits.
 * 3. Handle workers (look for terminated jobs and process their results)
 * 4. Check for messages in the database that the client should process.
 * 
 * @param grid_queue_id The id (DB primary key) of the grid the client is running on.
 */
void process_jobs(int grid_queue_id) {
    // fetch grid queue information from DB
    GridQueue grid_queue;
    if (get_grid_queue_info(grid_queue_id, grid_queue) != 1) {
        log_error(AT, "Couldn't retrieve grid information");
        exit_client(1);
    }
    log_message(LOG_INFO, "Retrieved grid queue information. Running on %s "
                          "with %d CPUs per node.\n\n", grid_queue.name.c_str(), grid_queue.numCPUs);
    
    if (grid_queue.numCores != host_info.num_cores || grid_queue.cpu_model != host_info.cpu_model) {
        log_message(LOG_IMPORTANT, "Warning! Client CPU model and grid queue CPU model differ!");
        if (!opt_run_on_inhomogenous_hosts) {
            log_message(LOG_IMPORTANT, "Option to continue running on inhomogenous hosts was NOT given. Exiting.");
            exit_client(0, false);
        }
        else {
            log_message(LOG_IMPORTANT, "Option to continue running on inhomogenous hosts was given. Proceeding.");
        }
    }
    
    // initialize worker slots
    initialize_workers(grid_queue);

    log_message(LOG_DEBUG, "Initialized %d worker slots. Starting main processing loop.\n\n", workers.size());
    
    unsigned int check_jobs_interval = opt_check_jobs_interval;
    while (true) {
        for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
            if (it->used == false) {
                if (!start_job(grid_queue_id, *it)) {
                    // if there's a free worker slot but a job couldn't be started
                    // we increase the check for jobs interval to reduce the load on the database
                    // especially when there are no more jobs left, double each time, cap at the hard-coded
                    // limit or the limit that was given as option
                    check_jobs_interval *= 2;
                    check_jobs_interval = min_(check_jobs_interval, max_(CHECK_JOBS_INTERVAL_UPPER_LIMIT, opt_check_jobs_interval));
                    break;
                }
                else {
                    check_jobs_interval = opt_check_jobs_interval;
                }
            }
        }
        
        bool any_running_jobs = false;
        for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
            any_running_jobs |= it->used;
        }
        if (!any_running_jobs && time(NULL) - t_started_last_job > opt_wait_jobs_time) {
            // got no jobs since opt_wait_jobs_time seconds and there aren't any jobs running.
            // Exit cleanly.
            log_message(LOG_INFO, "Didn't start any jobs within the last %d seconds and there "
                                        "are no jobs being processed currently. Exiting.", opt_wait_jobs_time);
            handle_workers(workers);
            exit_client(0, true);
        }
        
        int num_finished = handle_workers(workers);
        
        process_messages();
        if (num_finished == 0) {
            usleep(check_jobs_interval * 1000);
        }
    }
}

bool choose_experiment(int grid_queue_id, Experiment &chosen_exp) {
    // get list of possible experiments (those with the same grid queue
    // the client was started with)
    log_message(LOG_DEBUG, "Fetching list of experiments:");
    vector<Experiment> experiments;
    
    defer_signals();
    get_possible_experiments(grid_queue_id, experiments);
    reset_signal_handler();
    
    if (experiments.empty()) {
        log_message(LOG_DEBUG, "No experiments available");
        return false;
    }
    
    for (vector<Experiment>::iterator it = experiments.begin(); it != experiments.end(); ++it) {
        log_message(LOG_DEBUG, "%d %s %d", it->idExperiment, it->name.c_str(), it->priority);
    }
    
    log_message(LOG_DEBUG, "Fetching number of CPUs working on each experiment");
    map<int, int> cpu_count_by_experiment;
    
    defer_signals();
    get_experiment_cpu_count(cpu_count_by_experiment);
    reset_signal_handler();
    
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
    float max_diff = 0.0f;
    // find experiment with maximum (exp_prio / priority_sum - exp_cpus / sum_cpus)
    // i.e. maximum difference between target priority and current ratio of assigned CPUs
    for (vector<Experiment>::iterator it = experiments.begin(); it != experiments.end(); ++it) {
        float diff;
        if (priority_sum == 0 && sum_cpus == 0) {
            // all priorities 0, no CPUs used
            diff = 0.0f;
        }
        else if (priority_sum == 0) {
            // all experiments have priority 0, try to achieve uniform CPU distribution
            diff = (float)sum_cpus / (float)cpu_count_by_experiment[it->idExperiment];
        }
        else if (sum_cpus == 0) {
            // no clients processing jobs yet, choose the experiment with highest priority ratio
            diff = it->priority / (float)priority_sum;
        }
        else {
            diff = it->priority / (float)priority_sum - cpu_count_by_experiment[it->idExperiment] / (float)sum_cpus;
        }
        log_message(LOG_DEBUG, "Experiment %d - %s, prio: %d, CPU count: %d, diff: %.2f", 
                        it->idExperiment, it->name.c_str(), it->priority, cpu_count_by_experiment[it->idExperiment], diff);
        if (diff >= max_diff) {
            max_diff = diff;
            chosen_exp = *it;
        }
    }
    log_message(LOG_DEBUG, "Chose experiment %d - %s with difference %.2f",
                    chosen_exp.idExperiment, chosen_exp.name.c_str(), max_diff);
    return true;
}

/**
 * Try to find a job for the passed worker slot.
 * The following steps are performed:
 *
 * 1. Try to find an active experiment that has unprocessed jobs and try choose an experiment
 *    in a way that matches the priority of the experiment with the number of CPUs
 *    currently working on each experiment:
 *    This is done by evaluating over all possible experiments e:
 *    argmax { priority(e) / sum_priorities - CPUs(e) / sum_cpus }
 *    with the following special cases:
 *    - sum_priorities = 0 and sum_cpus = 0: choose any experiment
 *    - sum_priorities = 0: choose the experiment with the least amount of CPUs
 *    - sum_cpus = 0: choose the experiment with the highest priority
 *
 * 2. Try to fetch a job of the chosen experiment from the database. This can fail
 *    for multiple reasons, one of them being race conditions with our way of selecting
 *    a random row.
 *
 * 3. If a job was selected, the computational ressources (instance, solver, parameters) have to
 *    be retrieved from the database.
 *
 * 4. runsolver is started from a fork of the client process. The worker slot is
 *    set to used and the details of the started job are stored in the worker aswell.
 *
 * @param grid_queue_id the id of the grid the client is running on.
 * @param worker the worker slot which should manage the job run.
 * @return true on success, false if there are no jobs or the job query failed
 *         (e.g. for transaction race condition reasons)
 */
bool start_job(int grid_queue_id, Worker& worker) {
    log_message(LOG_DEBUG, "Trying to start processing a job");
    Experiment chosen_exp;
    if (!methods.choose_experiment(grid_queue_id, chosen_exp)) {
        return false;
    }
    
    Job job;
    job.solver_output_preserve_first = chosen_exp.solver_output_preserve_first;
    job.solver_output_preserve_last = chosen_exp.solver_output_preserve_last;
    job.watcher_output_preserve_first = chosen_exp.watcher_output_preserve_first;
    job.watcher_output_preserve_last = chosen_exp.watcher_output_preserve_last;
    job.verifier_output_preserve_first = chosen_exp.verifier_output_preserve_first;
    job.verifier_output_preserve_last = chosen_exp.verifier_output_preserve_last;
    job.limit_solver_output = chosen_exp.limit_solver_output;
    job.limit_watcher_output = chosen_exp.limit_watcher_output;
    job.limit_verifier_output = chosen_exp.limit_verifier_output;
    job.Cost_idCost = chosen_exp.Cost_idCost;

    defer_signals();
    int job_id = methods.db_fetch_job(client_id, grid_queue_id, chosen_exp.idExperiment, job);
    // keep track of jobs that were set to running in the DB but are still downloading resources/parameters
    // before a worker slot is actually assigned. This should prevent jobs from keeping the status running if
    // the client is killed (by other means than messages) while downloading resources.
    if (job_id != -1) downloading_job = job;
    reset_signal_handler();
    log_message(LOG_DEBUG, "Trying to fetch job, got %d", job_id);
    
    if (job_id != -1) {
        Solver solver;
        Instance instance;
        string instance_binary;
        string solver_base_path;
        
        ostringstream oss;
        oss << "Host information:" << endl;
        oss << setw(30) << "Number of cores: " << host_info.num_cores << endl;
        oss << setw(30) << "Number of threads: " << host_info.num_threads << endl;
        oss << setw(30) << "Hyperthreading: " << host_info.hyperthreading << endl;
        oss << setw(30) << "Turboboost: " << host_info.turboboost << endl;
        oss << setw(30) << "CPU model: " << host_info.cpu_model << endl;
        oss << setw(30) << "Cache size (KB): " << host_info.cache_size << endl;
        oss << setw(30) << "Total memory (MB): " << host_info.memory / 1024 / 1024 << endl;
        oss << setw(30) << "Free memory (MB): " << host_info.free_memory / 1024 / 1024 << endl << endl;
        job.launcherOutput = oss.str();
        defer_signals();
        methods.db_update_job(job);
        reset_signal_handler();

        log_message(LOG_DEBUG, "receiving solver informations");
        if (!get_solver(job, solver)) {
        	log_error(AT, "Could not receive solver information.");
        	job.status = -5;
            job.launcherOutput += get_log_tail();
            defer_signals();
            methods.db_update_job(job);
            reset_signal_handler();
            downloading_job.idJob = 0;
        	return false;
        }
        job.Solver_idSolver = solver.idSolver;

        log_message(LOG_DEBUG, "receiving instance informations");
        if (!get_instance(job, instance)) {
        	log_error(AT, "Could not receive instance information.");
        	job.status = -5;
            job.launcherOutput += get_log_tail();
            defer_signals();
            methods.db_update_job(job);
            reset_signal_handler();
            downloading_job.idJob = 0;
        	return false;
        }

        log_message(LOG_DEBUG, "checking instance binary");
        if (!get_instance_binary(instance, instance_binary)) {
        	log_error(AT, "Could not receive instance binary.");
        	job.status = -5;
            job.launcherOutput += get_log_tail();
            defer_signals();
            methods.db_update_job(job);
            reset_signal_handler();
            downloading_job.idJob = 0;
        	return false;
        }
        log_message(LOG_DEBUG, "checking solver binary");
        if (!get_solver_binary(solver, solver_base_path)) {
        	log_error(AT, "Could not receive solver binary.");
        	job.status = -5;
            job.launcherOutput += get_log_tail();
            defer_signals();
            methods.db_update_job(job);
            reset_signal_handler();
            downloading_job.idJob = 0;
        	return false;
        }

        log_message(LOG_IMPORTANT, "Solver binary at %s", solver_base_path.c_str());
        log_message(LOG_IMPORTANT, "Instance binary at %s", instance_binary.c_str());
        
        vector<Parameter> solver_parameters;
        defer_signals();
        if (get_solver_config_params(job.idSolverConfig, solver_parameters) != 1) {
            log_error(AT, "Could not receive solver config parameters");
            job.status = -5;
            job.launcherOutput = get_log_tail();
            methods.db_update_job(job);
            reset_signal_handler();
            downloading_job.idJob = 0;
            return false;
        }
        reset_signal_handler();
        string launch_command = "";
#ifdef use_hwloc
        if (!worker.core_ids.empty()) {
            ostringstream oss;
            oss << "taskset -c ";
            for (set<int>::iterator it = worker.core_ids.begin(); it != worker.core_ids.end(); it++) {
                oss << *it;
                if (++it != worker.core_ids.end()) {
                    oss << ',';
                }
                it--;
            }
            oss << " ";
            launch_command += oss.str();
        }
#endif
        launch_command += build_watcher_command(job);
        launch_command += " -- ";
        if (sandbox_command != "") {
			launch_command += sandbox_command;
			launch_command += " ";
        }
        ostringstream tempfiles_path;
        tempfiles_path << tempfiles_base_path << "/" << job.idJob << "/";
        if (!create_directory(tempfiles_path.str())) {
        	log_message(LOG_IMPORTANT, "Could not create temporary files directory for solver");
        }
        launch_command += build_solver_command(job, solver, solver_base_path, instance_binary, tempfiles_path.str(), solver_parameters);
        log_message(LOG_IMPORTANT, "Launching job with: %s", launch_command.c_str());
		
        // write some details about the job to the launcher output column
		
        oss << endl << endl;
		oss << "Job details:" << endl;
		oss << setw(30) << "idJob: " << job.idJob << endl;
		oss << setw(30) << "Solver: " << solver.solver_name << endl;
		oss << setw(30) << "Binary: " << solver.binaryName << endl;
		oss << setw(30) << "Launch command: " << launch_command << endl;
		oss << setw(30) << "Seed: " << job.seed << endl;
		oss << setw(30) << "Instance: " << instance.name << endl;
		job.launcherOutput = oss.str();
		
        int pid = fork();
        if (pid == 0) { // this is the child
            setpgrp();
            log_init_childprocess();
            defer_signals();
            char* command = new char[launch_command.length() + 1];
            strcpy(command, launch_command.c_str());
            char* exec_argv[4] = {strdup("/bin/bash") , strdup("-c"), command, NULL};

            // on errors: the child process sends the SIGKILL signal to itself.
            // This will cause the father process to set status of the job as watcher crash.
            if (chdir(absolute_path(solver_base_path).c_str()))
            {
                log_error(AT, "Couldn't cd into solver base path %s", absolute_path(solver_base_path).c_str());
                kill(getpid(), SIGKILL);
            }
            if (execve("/bin/bash", exec_argv, environp) == -1) {
                log_error(AT, "Error in execve()");
                kill(getpid(), SIGKILL);
            }
            kill(getpid(), SIGKILL); // should not be reached if execve doesn't fail
        }
        else if (pid > 0) {
            // fork was successful, this is the parent
            defer_signals();
            t_started_last_job = time(NULL);
			worker.used = true;
			worker.current_job = job; // this is a copy of the job, not a reference or pointer
			worker.current_job.instance_file_name = instance_binary;
            worker.pid = pid;
            downloading_job.idJob = 0; // 0 means there's no job for which the client is downloading resources at the moment
            methods.increment_core_count(client_id, chosen_exp.idExperiment);
            reset_signal_handler();
            return true;
        }
        else {
            log_error(AT, "Couldn't fork");
            exit_client(1);
        }
    }
    
    return false;
}

/**
 * Build a filename for the watcher output file.
 * @param job the job which to build the watcher output filename for.
 * @return a filename
 */
string get_watcher_output_filename(const Job& job) {
    ostringstream oss;
    oss << result_path << "/" << database_name << "_" << job.idJob << ".w";
    return oss.str();
}

/**
 * Build a filename for the solver output file.
 * @param job the job which to build the solver output filename for.
 * @return a filename
 */
string get_solver_output_filename(const Job& job) {
    ostringstream oss;
    oss << result_path << "/" << database_name << "_" << job.idJob << ".o";
    return oss.str();
}

/**
 * Builds the watcher command up to the point where the solver binary and
 * parameters should follow. The computational limits such as time and memory
 * limits are taken from the job. If the value of such a limit is -1, then no
 * limit is imposed.
 * 
 * e.g. "./runsolver --timestamp -w abc.w -o abc.o -C 1000"
 * Notice there's no trailing whitespace!
 * 
 * @param job The job that the watcher should launch
 * @return a command line string
 */
string build_watcher_command(const Job& job) {
    string watcher_out_file = get_watcher_output_filename(job);
    string solver_out_file = get_solver_output_filename(job);
    ostringstream cmd;
    cmd << absolute_path("./runsolver") << " --timestamp";
    cmd << " -w \"" << watcher_out_file << "\"";
    cmd << " -o \"" << solver_out_file << "\"";
    
    if (job.CPUTimeLimit != -1) cmd << " -C " << job.CPUTimeLimit;
    if (job.wallClockTimeLimit != -1) cmd << " -W " << job.wallClockTimeLimit;
    if (job.memoryLimit != -1) cmd << " -M " << job.memoryLimit;
    if (job.stackSizeLimit != -1) cmd << " -S " << job.stackSizeLimit;
    return cmd.str();
}

/**
 * Builds the solver launch command given the list of parameter instances.
 * The parameter vector should be passed in pre-sorted by the `order` column.
 * Parameters named `seed` and `instance` are special parameters that are always
 * substituted by the instance and seed values of the current job.
 * 
 * Example: "./solvers/TNM -seed 13456 -instance ./instances/in1.cnf -p1 1.2"
 * 
 * @param job the job that should be run
 * @param solver_binary_filename the filename of the solver binary
 * @param instance_binary_filename the filename of the instance
 * @param parameters a vector of Parameter instances that are used to build the command line arguments
 * @return command line string that runs the solver on the given instance
 */
string build_solver_command(const Job& job, const Solver& solver, const string& solver_base_path, 
                            const string& instance_binary_filename, const string& tempfiles_path,
                            const vector<Parameter>& parameters) {
    ostringstream cmd;
    cmd << solver.runCommand;
    if (solver.runCommand != "") cmd << " ";
    cmd << "\"" << solver_base_path << "/" << solver.runPath << "\" ";
    for (vector<Parameter>::const_iterator p = parameters.begin(); p != parameters.end(); ++p) {
        if (!p->attachToPrevious) cmd << " ";
        cmd << p->prefix;
        if (p->prefix != "") {
            if (p->space) { // space between prefix and value?
                cmd << " ";
            }
        }
        if (str_lower(p->name) == "seed") {
            cmd << job.seed;
        }
        else if (str_lower(p->name) == "instance") {
            cmd << "\"" << instance_binary_filename << "\"";
        }
        else if (str_lower(p->name) == "tempdir") {
        	cmd << "\"" << tempfiles_path << "/\"";
        }
        /*else if (p->name == "db_host") {
            cmd << get_db_host();
        }
        else if (p->name == "db_port") {
            cmd << get_db_port();
        }
        else if (p->name == "db_db") {
            cmd << get_db();
        }
        else if (p->name == "db_username") {
            cmd << get_db_username();
        }
        else if (p->name == "db_password") {
            cmd << get_db_password();
        }*/
        else {
            if (p->hasValue) {
                cmd << p->value;
            }
        }
    }
    return cmd.str();
}

/**
 * Builds the verifier launch command.
*/
string build_verifier_command(const Verifier& verifier, const string& verifier_base_path,
		  const string& output_solver, const string& instance, const string& output_watcher,
		  const string& output_launcher) {
    ostringstream cmd;
    cmd << verifier.runCommand;
    if (verifier.runCommand != "") cmd << " ";
    cmd << "\"" << verifier_base_path << "/" << verifier.runPath << "\" ";
    for (vector<VerifierParameter>::const_iterator p = verifier.parameters.begin(); p != verifier.parameters.end(); ++p) {
        if (!p->attachToPrevious) cmd << " ";
        cmd << p->prefix;
        if (p->prefix != "") {
            if (p->space) { // space between prefix and value?
                cmd << " ";
            }
        }
        if (str_lower(p->name) == "instance") {
        	cmd << "\"" << instance << "\"";
        }
        else if (str_lower(p->name) == "output_solver") {
            cmd << "\"" << output_solver << "\"";
        }
        else if (str_lower(p->name) == "output_launcher") {
            cmd << "\"" << output_launcher << "\"";
        }
        else if (str_lower(p->name) == "output_watcher") {
            cmd << "\"" << output_watcher << "\"";
        }
        else {
            if (p->hasValue) {
                cmd << p->value;
            }
        }
    }
    return cmd.str();
}

/**
 * Builds the cost binary launch command
 */
string build_cost_command(const Job& job, const CostBinary& cost_binary, const string& cost_binary_base_path, const string& output_solver, const string& instance) {
    ostringstream cmd;
    cmd << cost_binary.runCommand;
    if (cost_binary.runCommand != "") cmd << " ";
    cmd << "\"" << cost_binary_base_path << "/" << cost_binary.runPath << "\" ";
    string parameters(cost_binary.parameters);
    if (parameters.find("<output_solver>") != parameters.npos) {
    	parameters.replace(parameters.find("<output_solver>"), string("<output_solver>").length(), "\"" + output_solver + "\"");
    }
    if (parameters.find("<instance>") != parameters.npos) {
    	parameters.replace(parameters.find("<instance>"), string("<instance>").length(), "\"" + instance + "\"");
    }
    cmd << parameters;
    return cmd.str();
}

/**
 * Attempts to find the string @tokens in the stringstream @stream.
 * Consumes the passed in @stream so that is possible to extract tokens
 * following the found token string.
 * @param stream the stream
 * @param tokens the string
 * @return 1, if the tokens string was found, 0 if not
 */
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
        while (true) {
            stream >> s1;
            if (s1 == "#") {
                char c;
                while (stream.get(c) && c != '\n');
            } else break;
        }
		is >> s2;
		if (s1 != s2) {
            is.clear();
			is.seekg(0);
		}
	}
}

bool parse_watcher_line(istream &stream, const string prefix, float& value) {
	string line;
	while (std::getline(stream, line)) {
		if (line.substr(0, prefix.length()) == prefix) {
			istringstream ssline(line.substr(prefix.length(), line.length()));
			ssline >> value;
			return true;
		}
	}
	return false;
}

/**
 * Process the results of a given job. This includes
 * parsing the watcher (runsolver) output to determine if the solver
 * was terminated due to exceeding a computation limit or exited normally.
 * 
 * If it exited normally the verifier is run on the solver's output
 * and the instance used to determine a result code.
 * 
 * Notice: The results are not written back to the database by this function.
 * This should be done after calling this function.
 * 
 * @param job The job of which the results should be processed.
 * @return 1 on success, 0 on errors
 */
int process_results(Job& job) {    
    log_message(LOG_DEBUG, "Starting to process results of job %d", job.idJob);
	string watcher_output_filename = get_watcher_output_filename(job);
	string solver_output_filename = get_solver_output_filename(job);
    
	log_message(LOG_DEBUG, "Loading watcher output");
	if (!load_file_string(watcher_output_filename, job.watcherOutput)) {
		log_error(AT, "Could not read watcher output file.");
		return 0;
	}
	log_message(LOG_DEBUG, "Loading solver output");
	if (!load_file_binary(solver_output_filename, &job.solverOutput, &job.solverOutput_length)) {
		log_error(AT, "Could not read solver output file.");
		return 0;
	}
	log_message(LOG_DEBUG, "Starting to process results");

    stringstream ss(job.watcherOutput);
    float cputime;
    if (parse_watcher_line(ss, "CPU time (s):", cputime)) {
        job.resultTime = cputime;
    	job.status = 1;
    	log_message(LOG_IMPORTANT, "[Job %d] CPUTime: %f", job.idJob, job.resultTime);
    }
    ss.clear(); ss.seekg(0);
    float realtime;
    if (parse_watcher_line(ss, "Real time (s):", realtime)) {
    	job.wallTime = realtime;
    	job.status = 1;
    	log_message(LOG_IMPORTANT, "[Job %d] wall time: %f", job.idJob, job.wallTime);
    }
    job.resultCode = 0; // default result code is unknown

    ss.clear(); ss.seekg(0);
    if (find_in_stream(ss, "Maximum CPU time exceeded:")) {
        job.status = 21;
        job.resultCode = -21;
        log_message(LOG_IMPORTANT, "[Job %d] CPU time limit exceeded", job.idJob);
        return 1;
    }
    ss.clear(); ss.seekg(0);
    if (find_in_stream(ss, "Maximum wall clock time exceeded:")) {
        job.status = 22;
        job.resultCode = -22;
        log_message(LOG_IMPORTANT, "[Job %d] Wall clock time limit exceeded", job.idJob);
        return 1;
    }
    ss.clear(); ss.seekg(0);
    if (find_in_stream(ss, "Maximum VSize exceeded:")) {
        job.status = 23;
        job.resultCode = -23;
        log_message(LOG_IMPORTANT, "[Job %d] Memory limit exceeded", job.idJob);
        return 1;
    }

    // TODO: stack size limit

    ss.clear(); ss.seekg(0);
    if (find_in_stream(ss, "Child ended because it received signal")) {
    	int signal;
    	ss >> signal;
		if (signal == 24) { // SIGXCPU, this should be a normal timeout
			job.status = 21;
			job.resultCode = -21;
			log_message(LOG_IMPORTANT, "[Job %d] CPU time limit exceeded (received SIGXCPU)", job.idJob);
			return 1;
		}
		job.status = -3;
		job.resultCode = -(300+signal);
		log_message(LOG_IMPORTANT, "[Job %d] Received signal %d", job.idJob, signal);
		return 1;
    }
    
    ss.clear(); ss.seekg(0);
    if (find_in_stream(ss, "Child status: 126")) {
        job.status = -3;
        job.resultCode = -398;
        log_message(LOG_IMPORTANT, "[Job %d] runsolver couldn't execute solver binary", job.idJob);
        return 1;
    }
    
    ss.clear(); ss.seekg(0);
    if (find_in_stream(ss, "Child status: 127")) {
        job.status = -3;
        job.resultCode = -399;
        log_message(LOG_IMPORTANT, "[Job %d] runsolver couldn't execute solver binary", job.idJob);
        return 1;
    }

    if (job.status == 1) {
    	log_message(LOG_IMPORTANT, "[Job %d] Successful!", job.idJob);

    	char old_wd[PATH_MAX];
    	if (job.Cost_idCost != 0) {
			getcwd(old_wd, PATH_MAX);
			log_message(LOG_IMPORTANT, "[Job %d] running cost calculation!", job.idJob);
			CostBinary cost_binary;
			if (get_cost_binary_details(cost_binary, job.Solver_idSolver, job.Cost_idCost) == 0) {
				log_message(LOG_IMPORTANT, "[Job %d] couldn't get cost binary details.", job.idJob);
				job.launcherOutput += "\nCould not get cost binary details.\n";
			} else {
				string cost_binary_base_path;
				if (get_cost_binary(cost_binary, cost_binary_base_path) == 0) {
					log_message(LOG_IMPORTANT, "[Job %d] couldn't get cost binary.", job.idJob);
					job.launcherOutput += "\nCould not get cost binary.\n";
				} else {
					string cost_binary_command = build_cost_command(job, cost_binary, cost_binary_base_path, solver_output_filename, job.instance_file_name);
					if (chdir(cost_binary_base_path.c_str())) return 0; // TODO: do something on failure
					if (cost_binary_command != "") {
						FILE* cost_fd = popen(cost_binary_command.c_str(), "r");
						if (cost_fd == NULL) {
							log_error(AT, "Couldn't start cost_binary: %s", cost_binary_command.c_str());
							job.launcherOutput += "\nCouldn't start cost binary: " + cost_binary_command + "\n\n";
							job.launcherOutput += get_log_tail();
						}
						else {
							log_message(LOG_DEBUG, "Started cost binary %s", cost_binary_command.c_str());
							char buf[256];
							char* cost_binary_output = (char*)malloc(256 * sizeof(char));
							size_t max_len = 256;
							size_t len = 0;
							size_t n_read;
							while ((n_read = fread(buf, sizeof(char), 256, cost_fd)) > 0) {
								if (len + n_read >= max_len) {
									cost_binary_output = (char*)realloc(cost_binary_output, max_len * 2);
									max_len *= 2;
								}
								for (size_t i = 0; i < n_read; i++) {
									cost_binary_output[len+i] = buf[i];
								}
								len += n_read;
							}
							istringstream cbi(cost_binary_output);
							cbi >> job.cost;
							free(cost_binary_output);

							pclose(cost_fd);
						}
					}
				}
			}
			if (chdir(old_wd)) return 0; // TODO: do something on failure
    	}


    	Verifier verifier;
    	if (get_verifier_details(verifier, job.idExperiment) == 0) {
    		log_message(LOG_IMPORTANT, "[Job %d] couldn't get verifier details.", job.idJob);
    		job.launcherOutput += "\nCould not get verifier details.\n";
    		job.status = 1;
    		job.resultCode = 0;
    		return 1;
    	}

    	string verifier_base_path;
    	if (get_verifier_binary(verifier, verifier_base_path) == 0) {
    		log_message(LOG_IMPORTANT, "[Job %d] couldn't get verifier binary.", job.idJob);
    		job.launcherOutput += "\nCould not retrieve verifier binary.\n";
    		job.status = 1;
    		job.resultCode = 0;
    		return 1;
    	}

    	string verifier_command = build_verifier_command(verifier, verifier_base_path, solver_output_filename,
    			job.instance_file_name, watcher_output_filename, ""); // TODO: launcher output
        getcwd(old_wd, PATH_MAX);
        if (chdir(verifier_base_path.c_str())) return 0; // TODO: do something on failure


        // Run the verifier (if so configured) via popen which returns a file descriptor of
        // the verifier's stdout. Verifier output is read and stored in
        // the verifierOuput field of the job. The integer that is written after the last '\n'
        // in the verifier output is assumed to be the result code.
        if (verifier_command != "") {
            FILE* verifier_fd = popen(verifier_command.c_str(), "r");
            if (verifier_fd == NULL) {
                log_error(AT, "Couldn't start verifier: %s", verifier_command.c_str());
                job.launcherOutput += "\nCouldn't start verifier: " + verifier_command + "\n\n";
                job.launcherOutput += get_log_tail();
                // this is no reason to exit the client, the resultCode will simply remain
                // 0 = unknown
            }
            else {
                log_message(LOG_DEBUG, "Started verifier %s", verifier_command.c_str());
                char buf[256];
                char* verifier_output = (char*)malloc(256 * sizeof(char));
                size_t max_len = 256;
                size_t len = 0;
                size_t n_read;
                // read the output from the verifier's stdout into the temporary char* verifier_output
                while ((n_read = fread(buf, sizeof(char), 256, verifier_fd)) > 0) {
                    if (len + n_read >= max_len) {
                        verifier_output = (char*)realloc(verifier_output, max_len * 2);
                        max_len *= 2;
                    }
                    for (size_t i = 0; i < n_read; i++) {
                        verifier_output[len+i] = buf[i];
                    }
                    len += n_read;
                }
                // set the job's verifier output attributes (data + length)
                job.verifierOutput_length = len;
                job.verifierOutput = (char*) malloc(len * sizeof(char));
                memcpy(job.verifierOutput, verifier_output, len);
                free(verifier_output);
                
                int stat = pclose(verifier_fd);
                job.verifierExitCode = WEXITSTATUS(stat); // exit code of the verifier
                
                // read result code
                int pos = job.verifierOutput_length - 1;
                while (pos >= 0 && job.verifierOutput[pos] != '\n') pos--;
                if (pos >= 0) {
                    pos += 1; // skip newline character
                    char* res_code = new char[job.verifierOutput_length - pos + 1]; // length + 1 terminating zero byte
                    memcpy(res_code, job.verifierOutput + pos, job.verifierOutput_length - pos); 
                    res_code[job.verifierOutput_length - pos] = '\0';
                    if (job.resultCode == 0) job.resultCode = atoi(res_code);
                    free(res_code);
                }
                log_message(LOG_DEBUG, "Verifier exited with exit code %d", job.verifierExitCode);
            }
        }

        chdir(old_wd); // TODO: this has to be always executed!
    } else {
        log_message(LOG_DEBUG, "[Job %d] Not successful, status code: %d", job.idJob, job.status);
    }

    return 1;
}

/**
 * Handles the workers.
 * If a worker child process terminated, the results of its job are 
 * processed and written to the DB.
 * 
 * @param workers: vector of the workers
 * @return the number of jobs that finished
 */
int handle_workers(vector<Worker>& workers) {
    //log_message(LOG_DEBUG, "Handling workers");
    int num_finished = 0;
    for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
        if (it->used) {
            int child_pid = it->pid;
            if (it->pid == 0) {
                log_error(AT, "PID of an used worker can't be 0!");
                exit_client(1);
            }
            int proc_stat;
            
            int pid = waitpid(child_pid, &proc_stat, WNOHANG);
            if (pid == child_pid) {
                num_finished++;
                Job& job = it->current_job;
                if (WIFEXITED(proc_stat)) {
                    // normal watcher exit
                    job.watcherExitCode = WEXITSTATUS(proc_stat);
                    if (process_results(job) != 1) job.status = -5;
                    it->used = false;
                    it->pid = 0;

                    defer_signals();
                    decrement_core_count(client_id, it->current_job.idExperiment);
                    methods.db_update_job(job);
                    reset_signal_handler();
                }
                else if (WIFSIGNALED(proc_stat)) {
                    // watcher terminated with a signal
                    job.status = -400 - WTERMSIG(proc_stat);
                    job.resultCode = 0; // unknown result
                    it->used = false;
                    it->pid = 0;

                    defer_signals();
                    decrement_core_count(client_id, it->current_job.idExperiment);
                    methods.db_update_job(job);
                    reset_signal_handler();
                }
                else {
                    // TODO: can this happen?
                    log_error(AT, "reached an unexpected point in handle_workers, got proc_stat %d", proc_stat);
                    exit_client(1);
                }
                
                if (WIFEXITED(proc_stat) || WIFSIGNALED(proc_stat)) {
                    if (it->current_job.solverOutput != 0) free(it->current_job.solverOutput);
                    if (it->current_job.verifierOutput != 0) free(it->current_job.verifierOutput);
                    if (!opt_keep_output) {
                        if (remove(get_watcher_output_filename(job).c_str()) != 0) {
                            log_message(LOG_IMPORTANT, "Could not remove watcher output file %s", get_watcher_output_filename(job).c_str());
                        }
                        if (remove(get_solver_output_filename(job).c_str()) != 0) {
                            log_message(LOG_IMPORTANT, "Could not remove solver output file %s", get_solver_output_filename(job).c_str());
                        }
                    }
                    log_message(LOG_DEBUG, "Removing temporary directory of this run.");
                    ostringstream oss;
                    oss << "rm -rf " << tempfiles_base_path << "/" << job.idJob;
                    system(oss.str().c_str());
                }
            }
            if (pid == -1) {
                log_error(AT, "waitpid returned -1: errno %d", errno);
                exit_client(1);
            }
        }
    }
    return num_finished;
}

/**
 * Signs off the client from the database (deletes the client's row
 * in the Client table)
 */
void sign_off() {
    defer_signals();
	delete_client(client_id);
    reset_signal_handler();
    log_message(LOG_IMPORTANT, "Signed off");
}

/**
 * Strips off leading and trailing whitespace
 * from a string.
 * @param str string that should be trimmed
 * @return the trimmed string
 */
string trim_whitespace(const string& str) {
    size_t beg = 0;
    while (beg < str.length() && str[beg] == ' ') beg++;
    size_t end = str.length() - 1;
    while (end > 0 && str[end] == ' ') end--;
    return str.substr(beg, end - beg + 1);
}

/**
 * Reads the configuration file './config' that consists of lines of
 * key-value pairs separated by an '=' character into the references
 * that are passed in.
 * 
 * @param hostname The hostname (DNS/IP) of the database host.
 * @param username DB username
 * @param password DB password
 * @param database DB name
 * @param port DB port
 * @param grid_queue_id The id of the grid the client is running on.
 */
void read_config(string& hostname, string& username, string& password,
				 string& database, int& port, int& grid_queue_id,
				 string& jobserver_hostname, int& jobserver_port,
				 string& sandbox_command) {
	ifstream configfile(opt_config.c_str());
	if (!configfile.is_open()) {
		log_message(0, "Couldn't open config file. Make sure 'config' \
						exists in the client's working directory or the -c command line argument is valid ");
		return;
	}
    port = 3306; // hardcoded for now
	string line;
	while (getline(configfile, line)) {
		istringstream iss(line);
		string id, val, eq;
		iss >> id >> eq;
        size_t eq_pos = line.find_first_of("=", 0);
        val = trim_whitespace(line.substr(eq_pos + 1, line.length()));
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
        else if (id == "jobserver_host") {
            jobserver_hostname = val;
        }
        else if (id == "jobserver_port") {
            jobserver_port = atoi(val.c_str());
        }
        else if (id == "sandbox_command") {
        	sandbox_command = val;
        }
        else if (id == "solver_tempdir") {
            tempfiles_base_path = val;
        }
	}
	configfile.close();
}

/**
 * Client exit routine that kills or waits for any running jobs, cleans up,
 * signs off the client and then exits the program.
 * 
 * @param exitcode the exit code of the client
 * @param wait whether to wait for running jobs, default is false
 */
void exit_client(int exitcode, bool wait) {   
    if (simulate) {
        simulate_exit_client();
        return ;
    }
    if (wait) {
        bool jobs_running;
        do {
            handle_workers(workers);
            jobs_running = false;
            for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
                jobs_running |= it->used;
            }
            process_messages();
            usleep(100 * 1000); // 100 ms
        } while (jobs_running);
    }
    stop_message_thread();
    
    // This routine should not be interrupted by further signals, if possible
    defer_signals();
    // if there's a job for which the client was downloading resources and got interrupted before actually allocating
    // a worker slot, reset this job in the DB
    if (downloading_job.idJob != 0) {
        log_message(LOG_DEBUG, "Client killed while downloading resources for job %d. Resetting job to \"not started\"", downloading_job.idJob);
        db_reset_job(downloading_job.idJob);
    }
    
    // if there's still anything running, too bad!
    // first reset jobs (must be fast)
    for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
        if (it->used) {
			//it->current_job.launcherOutput = get_log_tail();
            //it->current_job.status = -5;
            //it->current_job.resultCode = 0;
            //methods.db_update_job(it->current_job);
            db_reset_job(it->current_job.idJob);
        }
    }

    // sign out
    sign_off();
    database_close();
    log_close();

    // then kill the solvers
    for (vector<Worker>::iterator it = workers.begin(); it != workers.end(); ++it) {
        if (it->used) {
            kill_process(it->pid);
        }
    }

    exit(exitcode);
}

/**
 * Print accepted command line parameters and other hints.
 */
void print_usage() {
    cout << "EDACC Client" << endl;
    cout << "------------" << endl;
    cout << endl;
    cout << "Usage: ./client [-v <verbosity>] [-l] [-w <wait for jobs time (s)>] [-i <handle workers interval (ms)>] [-k] [-b <path>] [-h] [-s]" << endl;
    // ------------------------------------------------------------------------------------X <-- last char here! (80 chars)
    cout << "Parameters:" << endl;
    cout << "  -v <verbosity>:                  integer value between 0 and 4 (from lowest " << endl <<
            "                                   to highest verbosity)" << endl;
    cout << "  -c <config file path>:           path of the configuration file. Defaults to " << endl <<
            "                                   ./config" << endl;
    cout << "  -l:                              if flag is set, the log output is written to" << endl <<
            "                                   a file instead of stdout." << endl;
    cout << "  -p <path>:                       if log output should be written to a file " << endl <<
            "                                   this is the path for the file. Defaults to " << endl <<
            "                                   working directory." << endl;
    cout << "  -w <wait for jobs time (s)>:     how long the client should wait for jobs " << endl <<
            "                                   after it didn't get any new jobs before " << endl <<
            "                                   exiting." << endl;
    cout << "  -i <handle workers interval ms>: how long the client should wait after" << endl <<
            "                                   handling workers and before looking for a " << endl <<
            "                                   new job." << endl;
    cout << "  -k:                              whether to keep the solver and watcher " << endl <<
            "                                   output files after uploading to the DB. " << endl <<
            "                                   Default behaviour is to delete them." << endl;
    cout << "  -b <path>:                       base path for creating temporary directories" << endl <<
            "                                   and files." << endl;
    cout << "  -d <path>:                       download path for downloading solvers and " << endl <<
            "                                   instances. Use this option for shared file " << endl <<
            "                                   systems. Defaults to base path." << endl;
    cout << "  -h:                              toggles whether the client should continue " << endl <<
            "                                   to run even though the CPU hardware of the " << endl <<
            "                                   grid queue is not homogenous." << endl;
    cout << "  -s:                              simulation mode: don't write anything to the" << endl <<
            "                                   db." << endl;
    cout << "  -t <walltime>:                   expects walltime in the format [[[d:]h:]m:]s." << endl;
    cout << endl;
    cout << COMPILATION_TIME << endl;
}

/**
 * Signal handler for the signals defined in signals.h
 * that the client receives.
 * We want to make sure the client exits cleanly and signs off from the
 * database.
 * 
 * @param signal the number of the signal that was received
 */
void signal_handler(int signal) {
    log_message(LOG_IMPORTANT, "Caught signal %d", signal);
    //if (signal == SIGINT) {
     //   exit_client(signal);
    //}
    exit_client(signal);
}

void update_wait_jobs_time(time_t new_wait_time) {
    opt_wait_jobs_time = new_wait_time;
    t_started_last_job = time(NULL);
}

string str_lower(const string& str) {
    string lwr(str);
    transform(lwr.begin(), lwr.end(), lwr.begin(), ::tolower);
    return lwr;
}
