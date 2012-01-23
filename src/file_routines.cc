#include <dirent.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>

#include "file_routines.h"
#include "md5sum.h"
#include "log.h"
using namespace std;

/**
 * Renames thes file/directory pointed at by <code>old_path</code> into <code>new_path</code>
 * 
 * @return 1 on success, 0 on errors
 */
int rename(const string& old_path, const string& new_path) {
    return rename(old_path.c_str(), new_path.c_str()) == 0;
}

/**
 * Creates the directory <code>path</code>.
 * 
 * @return 1 on success, 0 on errors
 */
int create_directory(const string& path) {
    if (file_exists(path)) return 1;
    return (mkdir(path.c_str(), 0777)) == 0;
}

/**
 * Checks whether the file <code>fileName</code> exists.
 * @param fileName path of the file
 * @return 1 if the file exists, 0 if not.
 */
int file_exists(const string& fileName) {
    if (access(fileName.c_str(), F_OK) == 0) {
        return 1;
    }
    return 0;
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

	unsigned int written = fwrite(content, sizeof(char), contentLen, dst);
    if (written != contentLen) {
        log_error(AT, "Error writing to file");
        return 0;
    }
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

/**
 * returns the absolute path of <code>path</code>.
 */
string absolute_path(string path) {
    char* resolved_path = 0;
    if ((resolved_path = realpath(path.c_str(), NULL)) == NULL) {
        log_error(AT, "Couldn't convert path to absolute path: %s", path.c_str());
        return "";
    }
    
    string abs_path =  string(resolved_path);
    free(resolved_path);
    return abs_path;
}

/**
 * Copies file from <code>from</code> to <code>to</code>.
 * @param from path of file to be copied
 * @param to path of destination file
 * @return
 */
int copy_file(string from, string to) {
    ifstream ifs(from.c_str(), std::ios::binary);
    if (ifs.fail()) {
        return 0;
    }
    std::ofstream ofs(to.c_str(), std::ios::binary);
    if (ofs.fail()) {
        ifs.close();
        return 0;
    }
    ofs << ifs.rdbuf();
    ifs.close();
    ofs.close();
    if (ifs.fail() || ofs.fail()) {
        return 0;
    }
    return 1;
}

int is_directory(string path) {
    struct stat st;
    if (stat(path.c_str(),&st) != 0) {
        return 0;
    }
    return S_ISDIR(st.st_mode);
}

/**
 * Copies the contents of the directory <code>from</code> to the directory <code>to</code>\n
 * If <code>to</code> does not exist it will be created.
 * @param from
 * @param to
 * @return
 */
int copy_directory(string from, string to) {
    if (!is_directory(from) || !create_directory(to)) {
        return 0;
    }
    DIR *dp;
    struct dirent *dirp;
    if ((dp = opendir(from.c_str())) == NULL) {
        log_error(AT, "Error (%d) opening directory %s", errno, from.c_str());
        return 0;
    }

    while ((dirp = readdir(dp)) != NULL) {
        string file = string(dirp->d_name);

        if (dirp->d_type == DT_DIR) {
            // directory
            if (file == "." || file == "..") {
                continue;
            }
            copy_directory(from + "/" + file, to + "/" + file);
        } else if (dirp->d_type == DT_REG) {
            // regular file
            copy_file(from + "/" + file, to + "/" + file);
        }
    }
    closedir(dp);
    return 1;
}

