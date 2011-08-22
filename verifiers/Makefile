.PHONY: all clean

all: ../bin/verifiers/SAT

clean:
	rm -f *.o
	rm -f ../bin/verifiers/SAT

../bin/verifiers/SAT: SAT.cc
	g++ SAT.cc -o ../bin/verifiers/SAT -O2 -static
