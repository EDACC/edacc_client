#include <cstdio>
#include <string>
#include <iostream>
#include <cstring>
#include <fstream>

#include <stropts.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/sysinfo.h>
#include <linux/netdevice.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include "host_info.h"

using std::string;
using std::ifstream;

/**
 * Returns the number of CPUs (or logical) processors
 * of the system.
 * Returns 0 on errors.
 */
int get_num_cpus() {
    ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo) {
        // TODO: log error
        return 0;
    }
    
    int num_cpus = 0;
    string s;
    while (getline(cpuinfo, s)) {
        if (s.substr(0, 7) == "core id") num_cpus++;
    }
    cpuinfo.close();
    return num_cpus;
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

        // simply return first ip that is not 127.0.0.1 for now
        if (strcmp(ip, "127.0.0.1") != 0) {
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
