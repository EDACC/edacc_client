#ifndef __file_routines_h__
#define __file_routines_h__
#include <stdlib.h>
#include <string>

using std::string;

int rename(const string& old_path, const string& new_path);
int create_directory(const string& path);
int file_exists(const string& fileName);
int check_md5sum(string& filename, string& md5);
int copy_data_to_file(string& fileName, const char* content, size_t contentLen, mode_t mode);
int load_file_string(string& filename, string& result);
int load_file_binary(string &filename, char** result, unsigned long* size);
string absolute_path(string path);
int is_directory(string path);
string extract_directory(const string& path);
int create_directories(const string& path);
int copy_file(string from, string to);
int copy_directory(string from, string to);
#endif
