#ifndef __log_h__
#define __log_h__

#include <string>
using std::string;

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define AT __FILE__ ":" TOSTRING(__LINE__)

extern int log_init(string filename, int verbosity);
extern void log_close();

extern void log_message(int verbosity, const char* format, ...);
extern void log_error(const char* location, const char* format, ...);

#endif
