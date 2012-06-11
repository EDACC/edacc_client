#ifndef __process_h__
#define __process_h__

bool kill_process(pid_t pid);
bool kill_process(pid_t pid, int wait_upto);

#endif
