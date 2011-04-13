
#include <sys/stat.h>
#include <fstream>
#include <iostream>

#include "file_routines.h"
#include "md5sum.h"
#include "log.h"
using namespace std;

extern string instance_path;
extern string solver_path;
extern string result_path;

int create_directories() {
	return (!file_exists(instance_path) && mkdir(instance_path.c_str(), 0777)
			|| !file_exists(solver_path) && mkdir(solver_path.c_str(), 0777)
			|| !file_exists(result_path) && mkdir(result_path.c_str(), 0777)) == 0;
}

int file_exists(string& fileName) {
	struct stat buf;
	if (stat(fileName.c_str(), &buf) == -1 && errno == ENOENT)
		return 0;
	return 1;
}

int check_md5sum(string& filename, string& md5) {
	if (!file_exists(filename)) return 0;
	unsigned char md5Buffer[16];
	char md5String[33];
	char* md5StringPtr;
	FILE* dst = fopen(filename.c_str(), "r");

	if (md5_stream(dst, &md5Buffer) != 0) {
		log_error(AT, "Error in md5_stream()\n");
		fclose(dst);
		return 0;
	}
	int i;
	for (i = 0, md5StringPtr = md5String; i < 16; ++i, md5StringPtr += 2)
		sprintf(md5StringPtr, "%02x", md5Buffer[i]);
	md5String[32] = '\0';
	int posDiff = strcmp(md5String, md5.c_str());
	if (posDiff != 0) {
		log_error(AT,
				"\nThere might be a problem with the md5 sums for file: %s\n",
				filename.c_str());
		log_error(AT, "%20s = %s\n", "DB md5 sum", md5.c_str());
		log_error(AT, "%20s = %s\n", "Computed md5 sum", md5String);
		log_error(AT, "position where they start to differ = %d\n", posDiff);
	}
	fclose(dst);
	return posDiff == 0;
}

int copy_data_to_file(string& fileName, const char* content, size_t contentLen, mode_t mode) {
	//Create the file
	FILE* dst = fopen(fileName.c_str(), "w+");
	if (dst == NULL) {
		log_error(AT, " Unable to open %s: %s\n", fileName.c_str(), strerror(errno));
		return 0;
	}

	fwrite(content, sizeof(char), contentLen, dst);
	fclose(dst);

	//Set the file permissions
	if (chmod(fileName.c_str(), mode) == -1) {
		log_error(AT, "Unable to change permissions for %s: %s\n", fileName.c_str(),
				strerror(errno));
		return 0;
	}

	return 1;
}

int load_file(string& filename, string& result) {
	ifstream infile(filename.c_str());
	if (!infile) {
		log_error(AT, "Error: Not able to open file: %s\n", filename.c_str());
		return 0;
	}
	string line;
	while (getline(infile, line)) {
		result += line + '\n';
	}
	infile.close();
	return 1;
}
