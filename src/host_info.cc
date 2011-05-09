#include <cstdio>
#include <string>
#include <iostream>
#include <cstring>
#include <fstream>
#include <sstream>
#include <set>

#include <stropts.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/sysinfo.h>
#include <linux/netdevice.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include "host_info.h"
#include "log.h"

using std::string;
using std::ifstream;
using std::stringstream;
using std::set;

/**
 * Returns the number of CPUs of the system.
 * Returns 0 on errors.
 */
int get_num_physical_cpus() {
    ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo) {
        log_error(AT, "Couldn't open /proc/cpuinfo");
        return 0;
    }
    
    string s;
    set<int> cores;
    set<int> physical_ids;
    while (getline(cpuinfo, s)) {
        if (s.substr(0, 11) == "physical id") {
            size_t colon_pos = s.find_first_of(":");
            if (colon_pos == string::npos) return 0;
            stringstream ss(s.substr(colon_pos+2, s.length() - (colon_pos+2) + 1));
            int physical_id = 0;
            ss >> physical_id;
            physical_ids.insert(physical_id);
        }
        if (s.substr(0, 7) == "core id") {
            size_t colon_pos = s.find_first_of(":");
            if (colon_pos == string::npos) return 0;
            stringstream ss(s.substr(colon_pos+2, s.length() - (colon_pos+2) + 1));
            int core_id = 0;
            ss >> core_id;
            cores.insert(core_id);
        }
    }
    cpuinfo.close();
    return cores.size() * physical_ids.size();
}

/**
 * Returns the number of logical processors of the system
 * Returns 0 on errors.
 */
int get_num_processors() {
    ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo) {
        log_error(AT, "Couldn't open /proc/cpuinfo");
        return 0;
    }
    
    int num_processors = 0;
    string s;
    while (getline(cpuinfo, s)) {
        if (s.substr(0, 9) == "processor") num_processors++;
    }
    cpuinfo.close();
    return num_processors;
}

/**
 * Returns whether the system's processors are hyperthreaded or not.
 * Returns false on errors.
 */
bool has_hyperthreading() {
    ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo) {
        log_error(AT, "Couldn't open /proc/cpuinfo");
        return false;
    }
    
    string s;
    while (getline(cpuinfo, s)) {
        if (s.substr(0, 5) == "flags" && s.find(" ht ") != string::npos) {
            return true;
        }
    }
    return false;
}

/**
 * Returns whether the CPUs support turboboost.
 * Returns false on errors.
 */
bool has_turboboost() {
    ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo) {
        log_error(AT, "Couldn't open /proc/cpuinfo");
        return false;
    }
    
    string s;
    while (getline(cpuinfo, s)) {
        if (s.substr(0, 5) == "flags" && s.find(" ida") != string::npos) {
            return true;
        }
    }
    return false;
}

/** 
 * Returns the CPU model name.
 * Returns "" on errors.
 */
string get_cpu_model() {
    ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo) {
        log_error(AT, "Couldn't open /proc/cpuinfo");
        return "";
    }
    
    string s;
    while (getline(cpuinfo, s)) {
        if (s.substr(0, 10) == "model name") {
            size_t colon_pos = s.find_first_of(":");
            if (colon_pos == string::npos) return "";
            return s.substr(colon_pos+2, string::npos);
        }
    }
    return "";
}

/**
 * Returns the cache size of the CPUs in KB (assuming KB is what
 * /proc/cpuinfo always specifies)
 * Returns 0 on errors.
 */
int get_cache_size() {
    ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo) {
        log_error(AT, "Couldn't open /proc/cpuinfo");
        return 0;
    }
    
    string s;
    while (getline(cpuinfo, s)) {
        if (s.substr(0, 10) == "cache size") {
            size_t colon_pos = s.find_first_of(":");
            if (colon_pos == string::npos) return 0;
            stringstream ss(s.substr(colon_pos+2, s.length() - (colon_pos+2) + 1 - 2));
            int cache_size = 0;
            ss >> cache_size;
            return cache_size;
        }
    }
    return 0;
}

/**
 * Returns the cpu flags of the system's CPU
 * Returns "" on errors.
 */
string get_cpu_flags() {
    ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo) {
        log_error(AT, "Couldn't open /proc/cpuinfo");
        return "";
    }
    
    string s;
    while (getline(cpuinfo, s)) {
        if (s.substr(0, 5) == "flags") {
            size_t colon_pos = s.find_first_of(":");
            if (colon_pos == string::npos) return "";
            return s.substr(colon_pos+2, string::npos);
        }
    }
    return "";
}

/**
 * Returns the content of /proc/cpuinfo as string.
 * Returns "" on errors.
 */
string get_cpuinfo() {
    ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo) {
        log_error(AT, "Couldn't open /proc/cpuinfo");
        return "";
    }
    
    return string((std::istreambuf_iterator<char>(cpuinfo)),
                    std::istreambuf_iterator<char>());
}

/**
 * Returns the content of /proc/meminfo as string.
 * Returns "" on errors.
 */
string get_meminfo() {
    ifstream meminfo("/proc/meminfo");
    if (!meminfo) {
        log_error(AT, "Couldn't open /proc/meminfo");
        return "";
    }
    
    return string((std::istreambuf_iterator<char>(meminfo)),
                    std::istreambuf_iterator<char>());
}

/**
 * Tries to find the IP address of the system. If there
 * is more than one interface, this returns the first
 * address != 127.0.0.1
 * 
 * @ipv6: whether to check ipv4 (when false) or ipv6
 * interfaces (when true).
 *
 * Returns "" on errors, ip address as string on success.
 */
string get_ip_address(bool ipv6) {
    int s;
    struct ifconf ifconf;
    struct ifreq ifr[50];
    int ifs;
    int i;
    
    int domain = AF_INET;
    if (ipv6) domain = AF_INET6;

    s = socket(domain, SOCK_STREAM, 0);
    if (s < 0) {
        perror("socket");
        return "";
    }

    ifconf.ifc_buf = (char *) ifr;
    ifconf.ifc_len = sizeof(ifr);

    if (ioctl(s, SIOCGIFCONF, &ifconf) == -1) {
        perror("ioctl");
        close(s);
        return "";
    }
    
    if (sizeof(ifr[0]) == 0) return ""; // no interfaces (?)

    ifs = ifconf.ifc_len / sizeof(ifr[0]);
    for (i = 0; i < ifs; i++) {
        char ip[INET_ADDRSTRLEN];
        struct sockaddr_in *s_in = (struct sockaddr_in *) &ifr[i].ifr_addr;

        if (!inet_ntop(domain, &s_in->sin_addr, ip, sizeof(ip))) {
            perror("inet_ntop");
            close(s);
            return "";
        }

        // simply return first ip that is not 127.0.0.x for now
        if (string(ip).substr(0, 8) != "127.0.0.") { 
            close(s);
            return string(ip);
        }
    }
    close(s);
    return "";
}

/**
 * Returns the hostname of the system.
 * Returns "" on errors.
 */
string get_hostname() {
    char buffer[512];
    buffer[511] = '\0';
    if (gethostname(buffer, 512) == -1) {
        perror("gethostbyname");
        return "";
    }
    return string(buffer);
}

/**
 * Returns the amount of system memory in bytes.
 */
unsigned long long int get_system_memory() {
    struct sysinfo info;
    if (sysinfo(&info) != 0) {
        perror("sysinfo");
        return 0;
    }
    return info.totalram;
}

/**
 * Returns the amount of free system memory in bytes.
 */
unsigned long long int get_free_system_memory() {
    struct sysinfo info;
    if (sysinfo(&info) != 0) {
        perror("sysinfo");
        return 0;
    }
    return info.freeram;
}
