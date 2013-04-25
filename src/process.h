#ifndef __process_h__
#define __process_h__

#include <vector>

bool get_process_pids(pid_t pid, std::vector<pid_t>& children);
bool kill_process(pid_t pid);
bool kill_process(pid_t pid, int wait_upto);

#endif
