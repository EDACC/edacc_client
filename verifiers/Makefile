all: SAT

clean:
	rm -f *.o
	rm -f ../bin/verifiers/SAT

SAT: SAT.cc
	g++ SAT.cc -o ../bin/verifiers/SAT -O2 -static
