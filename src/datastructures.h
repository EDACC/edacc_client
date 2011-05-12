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
	int outputSizeLimitFirst;
	int outputSizeLimitLast;
	
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
    
    Job() : idJob(0), idSolverConfig(0), idExperiment(0), idInstance(0),
            run(0), seed(0), status(0), startTime(""), resultTime(0.0), resultCode(0),
            computeQueue(0), priority(0), computeNode(""), computeNodeIP(""),
            watcherOutput(""), launcherOutput(""), solverExitCode(0), watcherExitCode(0),
            verifierExitCode(0), solverOutput(0), solverOutput_length(0), 
            verifierOutput(0), verifierOutput_length(0) {}
    
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
    
    string value;
};

class Worker {
public:
    int pid;
    bool used;
    Job current_job;
    
    Worker() : pid(0), used(false) {
    }
};

class Experiment {
public:
	int idExperiment;
	string name;
	int priority;
    
    Experiment() : idExperiment(0), name(""), priority(0) {}
    
    Experiment(int idExperiment, string name, int priority) :
            idExperiment(idExperiment), name(name), priority(priority) {}
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

#endif
