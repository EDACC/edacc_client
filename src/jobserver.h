/*
 * jobserver.hpp
 *
 *  Created on: 05.10.2011
 *      Author: simon
 */

#ifndef JOBSERVER_H_
#define JOBSERVER_H_

#include <string>
using namespace std;
class Jobserver {
private:
    bool connected;
    int fd;
    string hostname;
    string database;
    string username;
    string password;
    int port;
    bool checkConnection();
public:
    Jobserver(string hostname, string database, string username, string password, int port);
    bool connectToJobserver();
    bool getPossibleExperimentIds(int grid_queue_id, string &ids);
    bool getJobId(int experiment_id, int &idJob);
};

#endif /* JOBSERVER_HPP_ */
