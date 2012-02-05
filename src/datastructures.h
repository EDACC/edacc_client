#ifndef __datastructures_h__
#define __datastructures_h__

#include <string>
using std::string;

// forward declarations
class Worker;
class Experiment;
class Job;
class GridQueue;

class Job {
public:
	int idJob;
	int idSolverConfig, idExperiment, idInstance;
	int run;
	int seed;
	int status;
	string startTime;
	float resultTime;
	int resultCode;
	int computeQueue;
	int priority;
	string computeNode;
	string computeNodeIP;
	int CPUTimeLimit;
	int wallClockTimeLimit;
	int memoryLimit;
	int stackSizeLimit;
	
	string watcherOutput;
	string launcherOutput;
	int solverExitCode;
	int watcherExitCode;
	int verifierExitCode;
    
    char* solverOutput;
    unsigned long solverOutput_length;
    char* verifierOutput;
    unsigned long verifierOutput_length;
    
    string instance_file_name; // store this for easier access when running the verifier
    
    // these limit values are from the experiment table, but it is easier to have them here
    int solver_output_preserve_first, solver_output_preserve_last;
    int watcher_output_preserve_first, watcher_output_preserve_last;
    int verifier_output_preserve_first, verifier_output_preserve_last;
    bool limit_solver_output, limit_watcher_output, limit_verifier_output;

    Job() : idJob(0), idSolverConfig(0), idExperiment(0), idInstance(0),
            run(0), seed(0), status(0), startTime(""), resultTime(0.0), resultCode(0),
            computeQueue(0), priority(0), computeNode(""), computeNodeIP(""),
            watcherOutput(""), launcherOutput(""), solverExitCode(0), watcherExitCode(0),
            verifierExitCode(0), solverOutput(0), solverOutput_length(0), 
            verifierOutput(0), verifierOutput_length(0), solver_output_preserve_first(0),
            solver_output_preserve_last(0), watcher_output_preserve_first(0), watcher_output_preserve_last(0),
            verifier_output_preserve_first(0), verifier_output_preserve_last(0), limit_solver_output(false),
            limit_watcher_output(false), limit_verifier_output(false) {}
    
};

class HostInfo {
public:
    int num_cores;
    int num_threads;
    bool hyperthreading;
    bool turboboost;
    string cpu_model;
    int cache_size;
    string cpu_flags;
    unsigned long long int memory;
    unsigned long long int free_memory;
    string cpuinfo;
    string meminfo;
    
    HostInfo(): num_cores(0), num_threads(0), hyperthreading(false),
                turboboost(false), cache_size(0), memory(0),
                free_memory(0) {}
};

class Parameter {
public:
    int idParameter;
    string name;
    string prefix;
    bool hasValue;
    string defaultValue;
    int order;
    bool space;
    bool attachToPrevious;
    string value;
};

class Worker {
public:
    int pid;
#ifdef use_hwloc
    int core_id;
#endif
    bool used;
    Job current_job;
    
    Worker() : pid(0),
#ifdef use_hwloc
            core_id(-1),
#endif
        used(false) {
    }
};

class Experiment {
public:
	int idExperiment;
	string name;
	int priority;
	int solver_output_preserve_first, solver_output_preserve_last;
	int watcher_output_preserve_first, watcher_output_preserve_last;
	int verifier_output_preserve_first, verifier_output_preserve_last;
	bool limit_solver_output, limit_watcher_output, limit_verifier_output;
    
    Experiment() : idExperiment(0), name(""), priority(0), solver_output_preserve_first(0),
    		solver_output_preserve_last(0), watcher_output_preserve_first(0), watcher_output_preserve_last(0),
    		verifier_output_preserve_first(0), verifier_output_preserve_last(0), limit_solver_output(false),
    		limit_watcher_output(false), limit_verifier_output(false) {}
    
    Experiment(int idExperiment, string name, int priority, int solver_output_preserve_first,
    		int solver_output_preserve_last, int watcher_output_preserve_first, int watcher_output_preserve_last,
    		int verifier_output_preserve_first, int verifier_output_preserve_last, bool limit_solver_output,
    		bool limit_watcher_output, bool limit_verifier_output) :
            idExperiment(idExperiment), name(name), priority(priority),
            solver_output_preserve_first(solver_output_preserve_first),
            solver_output_preserve_last(solver_output_preserve_last),
            watcher_output_preserve_first(watcher_output_preserve_first),
            watcher_output_preserve_last(watcher_output_preserve_last),
            verifier_output_preserve_first(verifier_output_preserve_first),
            verifier_output_preserve_last(verifier_output_preserve_last),
            limit_solver_output(limit_solver_output),
            limit_watcher_output(limit_watcher_output),
            limit_verifier_output(limit_verifier_output) {}
};

class GridQueue {
public:
    int idgridQueue;
    string name;
    string location;
    int numCPUs;
    int numCores;
    string cpu_model;
    string description;
    
    GridQueue(): numCores(0) {}
};

class Solver {
public:
	int idSolverBinary;
	string solver_name;
	string binaryName;
	string md5;
    string runCommand;
    string runPath;
};

class Instance {
public:
	int idInstance;
	string name;
	string md5;
};

class Methods {
public:
    int (*sign_on) (int grid_queue_id);
    void (*sign_off) ();
    bool (*choose_experiment) (int grid_queue_id, Experiment &chosen_exp);

    int (*db_fetch_job) (int client_id, int grid_queue_id, int experiment_id, Job& job);
    int (*db_update_job)(const Job& job);
    int (*increment_core_count) (int client_id, int experiment_id);
};

#endif
