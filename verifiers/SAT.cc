#include <iostream>
#include <map>
#include <vector>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>

using namespace std;

int iabs(int a) { return a < 0 ? -a : a; }

void exit_verifier(int result_code, int exit_code) {
    cout << endl << result_code;
    exit(exit_code);
}

int main(int argc, char* argv[]) {
    ifstream instance(argv[1]);
    ifstream solver_output(argv[2]);
    string line;

    map<int, int> variables;
    bool SAT_answer = false;
    while (getline(solver_output, line)) {
        istringstream lss(line);
        string prefix;
        lss >> prefix;
        if (prefix == "s") {
            string answer;
            lss >> answer;
            if (answer == "UNKNOWN") {
                cout << "Solver reported unknown." << endl;
                exit_verifier(0, 0);
            }
            else if (answer == "SATISFIABLE") {
                cout << "Solver reported satisfiable. Checking." << endl;
                SAT_answer = true;
            }
            else if (answer == "UNSATISFIABLE") {
                cout << "Solver reported unsatisfiable. I guess it must be right!" << endl;
                exit_verifier(10, 0);
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
            int num_vars = 0;
            while (lss >> v && v != 0) {
                num_vars++;
                //if (variables.find(iabs(v)) == variables.end()) {
                //    sat_clause = false;
                //    break;
                //}
                if (variables[iabs(v)] == v) {
                    sat_clause = true;
                    break;
                }
            }
            if (!sat_clause && num_vars > 0) {
                cout << "Clause " << line << " not satisfied" << endl;
                cout << "Wrong solution." << endl;
                exit_verifier(-1, 0);
            }
        }
        cout << "Solution verified." << endl;
        exit_verifier(11, 0);
    }
    cout << "Didn't really find anything interesting in the output" << endl;
    exit_verifier(0, 0);
}
