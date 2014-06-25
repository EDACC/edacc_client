Compile by running "make" (requires g++)

Usage 
-----

./certifiedSAT14 <instance> <solveroutput>

Example
-------

./mySolver example.cnf > example_output.txt
./certifiedSAT14 example.cnf example_output.txt

The output should look something like:

    Solver reported unsatisfiable. Checking.
    Calling ./drat-trim example.cnf and piping solver output to stdin
    Reading output from checker.
    c reading proof from stdin
    c finished parsing
    c detected empty clause; start verification via backward checking
    c 8 of 8 clauses in core
    c 4 of 4 lemmas in core using 16 resolution steps
    c 0 RAT lemmas in core; 0 redundant literals in core lemmas
    Verification took 0 seconds. Checker output: s VERIFIED

    10                                                       
   
The "10" at the end is EDACC's code for a correct UNSAT proof.
