#ifndef __file_routines_h__
#define __file_routines_h__
#include <stdlib.h>
#include <string>

using std::string;

int create_directories();
int file_exists(string& fileName);
int check_md5sum(string& filename, string& md5);
int copy_data_to_file(string& fileName, const char* content, size_t contentLen, mode_t mode);
int load_file_string(string& filename, string& result);
int load_file_binary(string &filename, char** result, unsigned long* size);
#endif
