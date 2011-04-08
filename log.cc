#include <string>
#include <cstdarg>
#include <cstdio>
#include "log.h"

using std::string; 

FILE* logfile = stdout;
int log_verbosity = 0;

int log_init(string filename, int verbosity) {
    logfile = fopen(filename.c_str(), "a");
    if (logfile == 0) return 0;
    log_verbosity = verbosity;
    return 1;
}

void log_close() {
    fclose(logfile);
}

void log_message(int verbosity, const char* format, ...) {
    va_list args;
    if (verbosity <= log_verbosity) {
        va_start(args, format);
        vfprintf(logfile, format, args);
        fflush(logfile);
        va_end(args);
    }
}

void log_error(const char* location, const char* format, ...) {
    va_list args;
    va_start(args, format);
    fprintf(logfile, "ERROR at %s: ", location);
    vfprintf(logfile, format, args);
    va_end(args);
}
