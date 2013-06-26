.PHONY: all clean

all: certifiedSAT stc drup-check brup2drup

clean:
	rm -f *.o
	rm -f certifiedSAT stc drup-check brup2drup

certifiedSAT: certifiedSAT.cc 
	g++ certifiedSAT.cc -o certifiedSAT -O2 -static -g
	
stc: stc.c
	gcc stc.c -o stc -O2 -static -g
	
drup-check: drup-check.c
	gcc drup-check.c -o drup-check -O2 -static -g

brup2drup: brup2drup.c
	gcc brup2drup.c -o brup2drup -O2 -static -g
