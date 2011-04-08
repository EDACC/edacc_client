#ifndef __host_info_h__
#define __host_info_h__

#include <string>
using std::string;

extern int get_num_physical_cpus();
extern int get_num_processors();
extern bool has_hyperthreading();
extern bool has_turboboost();
extern string get_cpu_model();
extern int get_cache_size();
extern string get_cpu_flags();
extern string get_ip_address(bool ipv6);
extern string get_hostname();
extern unsigned long long int get_system_memory();

#endif
