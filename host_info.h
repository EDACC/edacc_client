#ifndef __host_info_h__
#define __host_info_h__

#include <string>
using std::string;

extern int get_num_cpus();
extern string get_ip_address(bool ipv6);
extern string get_hostname();
extern unsigned long long int get_system_memory();

#endif
