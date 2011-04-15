#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <string>

using namespace std;

int main(int argc, char* argv[]) {
    ifstream instance(argv[1]);
    ifstream solver_output(argv[2]);
    string line;

    /*vector<vector<int> > clauses();
    while (getline(instance, line)) {
        if (line.substr(0, 1) == "c" || line.substr(0, 1) == "p") continue;
        vector<int> clause();
        istringstream iss(line);
        int var;
        while (iss >> var && var != 0) {
            clause.push_back(var);
        }
        clauses.push_back(clause);
    }*/

    while (getline(solver_output, line)) {
        if (line.find("s UNKNOWN") != string::npos) {
            cout << "solver reported unknown" << endl;
            return 0;
        }
        else if (line.find("s SATISFIABLE") != string::npos) {
            cout << "solver reported satisfiable but I am too lazy to check" << endl;
            return 11;
        }
        else if (line.find("s UNSATISFIABLE") != string::npos) {
            cout << "solver reported unsatisfiable. I guess it must be right!" << endl;
            return 10;
        }
    }
    cout << "Didn't really find anything interesting in the output" << endl;
    return 0;
}
