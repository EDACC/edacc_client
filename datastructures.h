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
	int outputSizeLimit;
	
	string solverOutput;
	string watcherOutput;
	string launcherOutput;
	string verifierOutput;
	int solverExitCode;
	int watcherExitCode;
	int verifierExitCode;
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
    int numNodes;
    int numCPUs;
    int walltime;
    int availNodes;
    int maxJobsQueue;
    string description;

};

class Solver {
public:
	int idSolver;
	string name;
	string binaryName;
	string md5;
};

class Instance {
public:
	int idInstance;
	string name;
	string md5;
};

#endif
