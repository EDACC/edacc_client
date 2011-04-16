
#include <sys/stat.h>
#include <fstream>
#include <iostream>

#include "file_routines.h"
#include "md5sum.h"
#include "log.h"
using namespace std;

// declared in client.cc
extern string instance_path;
extern string solver_path;
extern string result_path;

/**
 * Creates the directories <code>instance_path</code>, <code>solver_path</code> and
 * <code>result_path</code>, if they don't exist yet.
 * 
 * @return 1 on success, 0 on errors
 */
int create_directories() {
	return ((!file_exists(instance_path) && mkdir(instance_path.c_str(), 0777))
			|| (!file_exists(solver_path) && mkdir(solver_path.c_str(), 0777))
			|| (!file_exists(result_path) && mkdir(result_path.c_str(), 0777))) == 0;
}

/**
 * Checks whether the file <code>fileName</code> exists.
 * @param fileName path of the file
 * @return 1 if the file exists, 0 if not.
 */
int file_exists(string& fileName) {
	struct stat buf;
	if (stat(fileName.c_str(), &buf) == -1 && errno == ENOENT)
		return 0;
	return 1;
}

/**
 * Returns whether the MD5 checksum of the file given by <code>filename</code>
 * matches the given <code>md5</code>
 * @param filename path of the file
 * @param md5 md5 checksum to be tested against
 * @return 1 if the md5 checksums match, 0 if not or on errors
 */
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

/**
 * Copies the data <code>content</code> of size <code>contentLen</code>
 * to a the file <code>fileName</code>. The file permissions of of <code>fileName</code>
 * are set to <code>mode</code> after writing the contents.
 * 
 * @param fileName path of the file
 * @param content char array with the contents to be written
 * @param contentLengh size of the content array
 * @param mode file permissions
 * @return 1 on success, 0 on errors
 */
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

/**
 * Loads a text file <code>filename</code> into the string reference <code>result</code>
 * 
 * @param filename path to the file
 * @param string reference to a string where the content should be put
 * @return 1 on success, 0 on errors
 */
int load_file_string(string& filename, string& result) {
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

/**
 * Loads the contents of a binary file <code>filename</code> (i.e. with 0 bytes) into
 * the char buffer <code>result</code>. The length of the data is put into <code>size</code>.
 * 
 * @param filename path of the file
 * @param result pointer to a char array where the content will be put
 * @param size pointer to the content size that will match the length of @result after loading the data
 * @return 1 on success, 0 on errors
 */
int load_file_binary(string &filename, char** result, unsigned long* size) {
	FILE *f = fopen(filename.c_str(), "rb");
	if (f == NULL) {
		*result = NULL;
		*size = 0;
		log_error(AT, "Error: Not able to open file: %s\n", filename.c_str());
		return 0;
	}
	fseek(f, 0, SEEK_END);
	*size = ftell(f);
	fseek(f, 0, SEEK_SET);
	*result = (char *) malloc(*size + 1);
	if (*size != fread(*result, sizeof(char), *size, f)) {
		*size = 0;
		free(*result);
		log_error(AT, "Error: Not able to read from file: %s\n", filename.c_str());
		return 0;
	}
	fclose(f);
	(*result)[*size] = 0;
	return 1;
}
