#include <string>
#include <vector>
#include <cstdarg>
#include <iostream>
#include <sstream>
#include <cstdio>
#include <time.h>

#include "log.h"

using std::string; 
using std::vector;
using std::ostringstream;
using std::endl;

FILE* logfile = stdout;
int log_verbosity = 0;

static const size_t log_tail_buffer_size = 100;
static vector<string> log_tail(0); // stores the last 100 lines that are written to the log

/**
 * Initializes the logging system with a given verbosity level
 * and a log filename.
 * When left uninitialized, log output goes to stdout and the 
 * verbosity level is 0.
 */
int log_init(string filename, int verbosity) {
    logfile = fopen(filename.c_str(), "a");
    if (logfile == 0) {
        logfile = stdout;
        return 0;
    }
    log_verbosity = verbosity;
    return 1;
}

/**
 * Closes the logfile.
 */
void log_close() {
    fclose(logfile);
}

string get_time() {
    time_t rawtime;
    struct tm * timeinfo;
    time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    string res = asctime(timeinfo);
    res.erase(res.size()-1,1);
    return res;
}

/**
 * Logs a message specified by the @format string and a list of
 * arguments if @verbosity is less than or equal to the logging verbosity
 * level.
 */
void log_message(int verbosity, const char* format, ...) {
	char buffer[4096];
    va_list args;
    if (verbosity <= log_verbosity) {
        va_start(args, format);
		
		size_t written = 0;
		written += snprintf(buffer, 4 + get_time().length(), "[%s] ", get_time().c_str());
		written += vsnprintf(buffer + written, sizeof(buffer) - written - 1, format, args);
		if (log_tail.size() == log_tail_buffer_size) log_tail.erase(log_tail.begin());
		log_tail.push_back(string(buffer));

		fprintf(logfile, buffer);
        fprintf(logfile, "\n");
		
        fflush(logfile);
        va_end(args);
    }
}

/**
 * Logs an error message that occured at @location (use the AT macro).
 * Error messages always get logged regardless of the verbosity level.
 */
void log_error(const char* location, const char* format, ...) {
    char buffer[4096];
    va_list args;
    va_start(args, format);
	
	size_t written = 0;
	written += snprintf(buffer, 4 + get_time().length(), "[%s] ", get_time().c_str());
	written += snprintf(buffer + written, sizeof(buffer) - written - 1, "Error at %s: ", location);
	written += vsnprintf(buffer + written, sizeof(buffer) - written - 1, format, args);
	if (log_tail.size() == log_tail_buffer_size) log_tail.erase(log_tail.begin());
	log_tail.push_back(string(buffer));
	
	fprintf(logfile, buffer);
    fprintf(logfile, "\n");

    va_end(args);
}

string get_log_tail() {
	ostringstream oss;
	for (vector<string>::iterator it = log_tail.begin(); it != log_tail.end(); ++it) {
		oss << *it << endl;
	}
	return oss.str();
}
