/*
 * database_fs_locking.h
 *
 *  Created on: 13.03.2012
 *      Author: simon
 */

#ifndef DATABASE_FS_LOCKING_H_
#define DATABASE_FS_LOCKING_H_

#include <string>

using std::string;

int update_file_lock(string &filename);
int lock_file(string &filename);
int unlock_file(string& filename);
int file_locked(string& filename);
void *update_file_lock_thread(void* ptr);

class File_lock_update {
public:
    string filename;
    int fsid;
    int finished;
};

#endif /* DATABASE_FS_LOCKING_H_ */
