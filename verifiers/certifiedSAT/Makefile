.PHONY: all clean

all: certifiedSAT certifiedSAT14 stc drup-check brup2drup drat-trim

clean:
	rm -f *.o
	rm -f certifiedSAT stc drup-check brup2drup drat-trim certifiedSAT14

certifiedSAT: certifiedSAT.cc 
	g++ certifiedSAT.cc -o certifiedSAT -O2 -static -g
	
certifiedSAT14: certifiedSAT14.cc 
	g++ certifiedSAT14.cc -o certifiedSAT14 -O2 -static -g

stc: stc.c
	gcc stc.c -o stc -O2 -static -g
	
drup-check: drup-check.c
	gcc drup-check.c -o drup-check -O2 -static -g

brup2drup: brup2drup.c
	gcc brup2drup.c -o brup2drup -O2 -static -g

drat-trim: drat-trim.c
	gcc -static -std=c99 -O2 -o drat-trim drat-trim.c
