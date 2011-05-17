#include <iostream>
#include <map>
#include <vector>
#include <fstream>
#include <sstream>
#include <string>

using namespace std;

int iabs(int a) { return a < 0 ? -a : a; }

int main(int argc, char* argv[]) {
    ifstream instance(argv[1]);
    ifstream solver_output(argv[2]);
    string line;

    map<int, int> variables;
    bool SAT_answer = false;
    while (getline(solver_output, line)) {
        istringstream lss(line);
        string timestamp, prefix;
        lss >> timestamp >> prefix; // read away timestamp
        if (prefix == "s") {
            string answer;
            lss >> answer;
            if (answer == "UNKNOWN") {
                cout << "Solver reported unknown." << endl;
                return 0;
            }
            else if (answer == "SATISFIABLE") {
                cout << "Solver reported satisfiable. Checking." << endl;
                SAT_answer = true;
            }
            else if (answer == "UNSATISFIABLE") {
                cout << "Solver reported unsatisfiable. I guess it must be right!" << endl;
                return 10;
            }
        }
        else if (prefix == "v") {
            // read solution
            int v;
            while (lss >> v && v != 0) {
                variables[iabs(v)] = v;
            }
        }
    }
    if (SAT_answer) {
        while (getline(instance, line)) {
            if (line.substr(0, 1) == "c" || line.substr(0, 1) == "p") continue;
            istringstream lss(line);
            int v;
            bool sat_clause = false;
            while (lss >> v && v != 0) {
                if (variables.find(iabs(v)) == variables.end()) {
                    sat_clause = false;
                    break;
                }
                if (variables[iabs(v)] == v) {
                    sat_clause = true;
                    break;
                }
            }
            if (!sat_clause) {
                cout << "Wrong solution." << endl;
                return 0;
            }
        }
        cout << "Solution verified." << endl;
        return 11;
    }
    cout << "Didn't really find anything interesting in the output" << endl;
    return 0;
}
