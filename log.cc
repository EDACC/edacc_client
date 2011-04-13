#include <string>
#include <cstdarg>
#include <cstdio>
#include "log.h"

using std::string; 

FILE* logfile = stdout;
int log_verbosity = 0;

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

/**
 * Logs a message specified by the @format string and a list of
 * arguments if @verbosity is less than or equal to the logging verbosity
 * level.
 */
void log_message(int verbosity, const char* format, ...) {
    va_list args;
    if (verbosity <= log_verbosity) {
        va_start(args, format);
        vfprintf(logfile, format, args);
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
    va_list args;
    va_start(args, format);
    fprintf(logfile, "ERROR at %s: ", location);
    vfprintf(logfile, format, args);
    fprintf(logfile, "\n");
    va_end(args);
}
