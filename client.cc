#include <iostream>
#include <cstdlib>
#include <unistd.h>
#include <getopt.h>

#include "host_info.h"
#include "log.h"

using std::cout;
using std::endl;

extern int optind;

void print_usage();

int main(int argc, char* argv[]) {
	static const struct option long_options[] = {
        { "verbosity", required_argument, 0, 'v' },
        { "solve_once", no_argument, 0, 's' },
        { "keep_results", no_argument, 0, 'k' },
        { "wait_for_db", required_argument, 0, 'w' },
        { "wait_for_jobs", required_argument, 0, 'j' }, 
        { "connect_attempts", required_argument, 0, 'c' },
        { "expID", required_argument, 0, 'e' }, 0 };

    int opt_verbosity;
    
	while (optind < argc) {
		int index = -1;
		struct option * opt = 0;
		int result = getopt_long(argc, argv, "v:skw:j:c:e:", long_options,
				&index);
		if (result == -1)
			break; /* end of list */
		switch (result) {
		case 'v':
			opt_verbosity = atoi(optarg);
			break;
		case 0: /* all parameter that do not */
			/* appear in the optstring */
			opt = (struct option *) &(long_options[index]);
			cout << opt->name << " was specified" << endl;
			if (opt->has_arg == required_argument)
                cout << "Arg: <" << optarg << ">" << endl;
            cout << endl;
			break;
		default:
			cout << "unknown parameter" << endl;
			print_usage();
			return 0;
		}
	}
    
    cout << "CPUs:        " << get_num_cpus() << endl;
    cout << "IP address:  " << get_ip_address(false) << endl;
    cout << "Hostname:    " << get_hostname() << endl;
    cout << "Memory (MB): " << get_system_memory() / 1024 / 1024 << endl;

    return 0;
}


void print_usage() {
    cout << "EDACC Client\n" << endl;
}
