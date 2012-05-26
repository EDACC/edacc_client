import sys

def iabs(x):
    return x if x >= 0 else -x

def exit_verifier(result_code, exit_code):
    print("\n" + str(result_code))
    sys.exit(exit_code)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print ("Usage: python PySAT.py <instance> <solver_output>")
        exit_verifier(0, 0)
    instance_path = sys.argv[1]
    solver_output_path = sys.argv[2]
    
    variables = {}
    SAT_answer = False
    with open(solver_output_path) as solver_output:
        for line in solver_output:
            tokens = line.split()
            if len(tokens) < 2: continue
            prefix = tokens[1]
            if prefix == "s":
                answer = tokens[2]
                if answer == "UNKNOWN":
                    print("Solver reported unknown.")
                    exit_verifier(0, 0)
                elif answer == "SATISFIABLE":
                    print("Solver reported satisfiable. Checking solution.")
                    SAT_answer = True
                elif answer == "UNSATISFIABLE":
                    print("Solver reported unsatisfiable. I guess it must be right!")
                    exit_verifier(10, 0)
            elif prefix == "v":
                for var in map(int, tokens[2:]): variables[iabs(var)] = var

    if SAT_answer:
        with open(instance_path) as instance:
            for line in instance:
                if line[0] in ('c', 'p'): continue
                clause = map(int, line.split()[:-1])
                if not clause or not any(variables[iabs(var)] == var for var in clause):
                    print("Clause", line,"not satisfied")
                    print("Wrong solution!")
                    exit_verifier(-1, 0)
        print("Solution verified.")
        exit_verifier(11, 0)
    else:
        print("Didn't find anything interesting in the output")
        exit_verifier(0, 0)
