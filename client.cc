#include <iostream>
#include <cstdlib>

#include "host_info.h"
#include "log.h"

using namespace std;

int main(int argc, char* argv[]) {
    cout << "CPUs:        " << get_num_cpus() << endl;
    cout << "IP address:  " << get_ip_address(false) << endl;
    cout << "Hostname:    " << get_hostname() << endl;
    cout << "Memory (MB): " << get_system_memory() / 1024 / 1024 << endl;

    return 0;
}
