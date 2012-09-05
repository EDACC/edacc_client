#include <sys/types.h>
#include <sys/wait.h>
#include <dirent.h>
#include <stdio.h>
#include <vector>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include "process.h"
#include "log.h"

using namespace std;

/**
 * Sends signal SIGTERM to <code>pid</code> and all of its children. Waits up to <code>wait_upto</code>
 * seconds before SIGKILL is sent.
 * @param pid
 * @return
 */
bool kill_process(pid_t pid, int wait_upto) {
	DIR *proc_dir;
	struct dirent *process_dir;
	pid_t c_pid, ppid;

	// first get all pids of the currently running processes
	if ((proc_dir = opendir("/proc")) == NULL) {
		log_error(AT, "Could not open /proc");
		return false;
	}
	vector < pid_t > pids;
	while ((process_dir = readdir(proc_dir)) != NULL) {
		if (isdigit(process_dir->d_name[0])) {
			c_pid = (pid_t) atoi(process_dir->d_name);
			pids.push_back(c_pid);
		}
	}

	// iterate over children and add the children of the children, and so on.
	vector < pid_t > children;
	children.push_back(pid);
	FILE *proc_file;
	for (unsigned int i = 0; i < children.size(); i++) {
		vector<pid_t>::const_iterator p;
		for (p = pids.begin(); p != pids.end(); p++) {
			char proc_filename[1024];
			sprintf(proc_filename, "/proc/%d/status", *p);
			if ((proc_file = fopen(proc_filename, "r")) != NULL) {
				ppid = -1;
				char line[81];
				while (ppid == -1 && fgets(line, 80, proc_file) != NULL) {
					sscanf(line, "PPid: %d", &ppid);
				}
				if (ppid == children[i]) {
					children.push_back(*p);
				}
				fclose(proc_file);
			}
		}
	}
	kill(pid, SIGTERM);

	// wait; check if pid is killed
	for (int i = 0; i < wait_upto; i++) {
		if (kill(pid, 0) != 0)
			break;
		sleep(1);
	}

	vector<pid_t>::reverse_iterator child;
	for (child = children.rbegin(); child != children.rend(); child++) {
		if (kill(*child, 0) == 0) {
			// the child isn't killed -> SIGKILL
			log_message(LOG_IMPORTANT, "Sending SIGKILL to %d", *child);
			kill(*child, SIGKILL);
		}
	}
	return true;
}

/**
 * Sends signal SIGTERM to <code>pid</code> and all of its children. Waits max. 2 sec until
 * SIGKILL is sent.
 * @param pid
 * @return
 */
bool kill_process(pid_t pid) {
	return kill_process(pid, 2);
}
