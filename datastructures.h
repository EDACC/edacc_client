#ifndef __datastructures_h__
#define __datastructures_h__

#include <string>
using std::string;

class Worker {
public:
    int pid;
    bool used;
    
    Worker() : pid(0), used(false) {
    }
};

class Experiment {
public:
	int idExperiment;
	string name;
	int priority;
    
    Experiment(int idExperiment, string name, int priority) :
            idExperiment(idExperiment), name(name), priority(priority) {}
};

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
	string dateModified;
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

#endif
