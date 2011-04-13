#include <iostream>
#include <fstream>
#include <string>

using namespace std;

int main(int argc, char* argv[]) {
    ifstream instance(argv[1]);
    ifstream solver_output(argv[2]);
    string line;
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
