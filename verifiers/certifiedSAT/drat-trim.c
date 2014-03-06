/*************************************************************************************[drup-trim.c]
Copyright (c) 2013, Marijn Heule, Nathan Wetzler, Anton Belov
Last edit, December 4, 2013

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT
OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
**************************************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/time.h>

#define TIMEOUT     20000
#define BIGINIT     1000000
#define INIT        8
#define END         0
#define UNSAT       0
#define SAT         1
#define EXTRA       3
#define MARK        3
#define ERROR      -1

#define CANDIDATE_INIT_SIZE 10
#define RAT_DEPENDENCIES_INIT_SIZE 10

struct solver { FILE *inputFile, *proofFile, *coreFile, *lemmaFile, *traceFile;
    int *DB, nVars, nClauses, timeout, mask, delete, *falseStack, *false, *forced,
      *processed, *assigned, count, *base, *used, *max, *delinfo, RATmode, RATcount,
      maxCandidates, *resolutionCandidates, maxRATdependencies, currentRATdependencies,
      *RATdependencies, *RATforced;
    struct timeval start_time;
    long mem_used, start, time, adsize, adlemmas, *reason, lemmas, arcs;  };

long **wlist;
long *adlist;
int *buffer;  // ANTON: to be used as local buffer in parse() and verify()

#define ASSIGN(a)	{ /* printf("  Assigning: %d\n", (a)); */ S->false[-(a)] = 1; *(S->assigned++) = -(a); }
#define ADD_WATCH(l,m)  { if (S->used[(l)] + 1 == S->max[(l)]) { S->max[(l)] *= 1.5; \
                            wlist[(l)] = (long *) realloc(wlist[(l)], sizeof(long) * S->max[(l)]); } \
                          wlist[(l)][ S->used[(l)]++ ] = (m); wlist[(l)][ S->used[(l)] ] = END; }

// +ANTON: convenience method for printing clauses
static inline void printClause(int* clause)
{
  while(*clause) { printf("%d ", *clause++); }
  printf("0\n");
}
// -ANTON

static inline void addWatch (struct solver* S, int* clause, int index) {
  int lit = clause[ index ];
  if (S->used[lit] + 1 == S->max[lit]) { S->max[lit] *= 1.5;
    wlist[lit] = (long*) realloc(wlist[lit], sizeof(long) * S->max[lit]); }
  wlist[lit][ S->used[lit]++ ] = ((long) (((clause) - S->DB)) << 1) + S->mask;
  wlist[lit][ S->used[lit]   ] = END; }

static inline void removeWatch (struct solver* S, int* clause, int index) {
  int lit = clause[index]; long *watch = wlist[lit];
  for (;;) {
    int* _clause = S->DB + (*(watch++) >> 1);
    if (_clause == clause) {
      watch[-1] = wlist[lit][ --S->used[lit] ];
      wlist[lit][ S->used[lit] ] = END; return; } } }

static inline void markWatch (struct solver* S, int* clause, int index, int offset) {
  long* watch = wlist[ clause[ index ] ];
  for (;;) {
    int *_clause = (S->DB + (*(watch++) >> 1) + (long) offset);
    if (_clause == clause) { watch[-1] |= 1; return; } } }

static inline void markClause (struct solver* S, int* clause, int index) {
  S->arcs++;

  if (S->traceFile) {
    if (S->RATmode) {
      if (S->currentRATdependencies == S->maxRATdependencies) {
	S->maxRATdependencies = (S->maxRATdependencies * 3) >> 1;
	S->RATdependencies = realloc(S->RATdependencies,
				     sizeof(int) * S->maxRATdependencies);
      }
      S->RATdependencies[S->currentRATdependencies] = clause[index - 1] >> 1;
      S->currentRATdependencies++;
    }
    else fprintf(S->traceFile, "%i ", clause[index - 1] >> 1);
  }

  if ((clause[index - 1] & 1) == 0) {
    clause[index - 1] |= 1;
    if (S->lemmaFile) {
      *(S->delinfo++) = S->time;
      *(S->delinfo++) = (int) (clause - S->DB) + index; }
    if (clause[1 + index] == 0) return;
    markWatch (S, clause,     index, -index);
    markWatch (S, clause, 1 + index, -index); }
  while (*clause) S->false[ *(clause++) ] = MARK; }

void analyze (struct solver* S, int* clause, int index) {     // Mark all clauses involved in conflict
  markClause (S, clause, index);
  while (S->assigned > S->falseStack) {
    int lit = *(--S->assigned);
    if ((S->false[ lit ] == MARK) &&
        (S->reason[ abs(lit) ]) ) {
      markClause (S, S->DB+S->reason[ abs(lit) ], -1);
      if (S->assigned >= S->RATforced)
        S->reason[ abs(lit) ] = 0;
    }
    S->false[ lit ] = (S->assigned < S->forced);
  }
  if (S->traceFile) {
    if (S->RATmode) {
    // fprintf(S->traceFile, "0 ");
    }
    else fprintf(S->traceFile, "0\n");
  }
  S->processed = S->assigned = S->forced; 
}

int propagate (struct solver* S) {                  // Performs unit propagation
  int *start[2], check = 0;
  int i, lit, _lit = 0; long *watch, *_watch;
  start[0] = start[1] = S->processed;
  flip_check:;
  check ^= 1;
  while (start[check] < S->assigned) {              // While unprocessed false literals
    lit = *(start[check]++);                        // Get first unprocessed literal
    if (lit == _lit) watch = _watch;
    else watch = wlist[ lit ];                      // Obtain the first watch pointer
    while (*watch != END) {                         // While there are watched clauses (watched by lit)
      if ((*watch & 1) != check) {
        watch++; continue; }
     int *clause = S->DB + (*watch / 2);	    // Get the clause from DB
     if (S->false[ -clause[0] ] ||
         S->false[ -clause[1] ]) {
       watch++; continue; }
     if (clause[0] == lit) clause[0] = clause[1];   // Ensure that the other watched literal is in front
      for (i = 2; clause[i]; ++i)                   // Scan the non-watched literals
        if (S->false[ clause[i] ] == 0) {           // When clause[j] is not false, it is either true or unset
          clause[1] = clause[i]; clause[i] = lit;   // Swap literals
          ADD_WATCH (clause[1], *watch);            // Add the watch to the list of clause[1]
          *watch = wlist[lit][ --S->used[lit] ];    // Remove pointer
          wlist[lit][ S->used[lit] ] = END;
          goto next_clause; }                       // Goto the next watched clause
      clause[1] = lit; watch++;                     // Set lit at clause[1] and set next watch
      if (!S->false[  clause[0] ]) {                // If the other watched literal is falsified,
        ASSIGN (clause[0]);                         // A unit clause is found, and the reason is set
        S->reason[abs(clause[0])] = ((long) ((clause)-S->DB)) + 1;
        if (!check) {
          start[0]--; _lit = lit; _watch = watch;
          goto flip_check; } }
      else { analyze(S, clause, 0); return UNSAT; } // Found a root level conflict -> UNSAT
      next_clause: ; } }                            // Set position for next clause
  if (check) goto flip_check;
  S->processed = S->assigned;
  return SAT; }	                                    // Finally, no conflict was found

int verify (struct solver *S) {
  long ad, d = 0;
  int flag, size;
  int *clause;
  int *lemmas = (S->DB + S->lemmas);
  int *last   = lemmas;
  int checked = S->adlemmas;
  int *delstack;

  S->time = lemmas[-1];
  if (S->lemmaFile) {
    delstack = (int *) malloc (sizeof(int) * S->count * 2);
    S->delinfo = delstack; }

  if (S->traceFile) fprintf(S->traceFile, "%i 0 ", S->count - 1);

  if (S->processed < S->assigned)
    if (propagate (S) == UNSAT) {
      printf("c got UNSAT propagating in the input instance\n");
      goto postprocess;
    }
  S->forced = S->processed;
  S->RATforced = S->forced;

  for (;;) {
    flag = size = 0;
    S->time = lemmas[-1];
    clause  = lemmas;

    do {
      ad = adlist[ checked++ ]; d = ad & 1;
      int* c = S->DB + (ad >> 1);
      if (d && c[1]) {  // if delete and not unit
        if (S->reason[ abs(c[0]) ] - 1 == (ad >> 1) ) continue;
        removeWatch(S, c, 0);
        removeWatch(S, c, 1); }
    }
    while (d);

    while (*lemmas) {
      int lit = *(lemmas++);
      if ( S->false[ -lit ]) flag = 1;
      if (!S->false[  lit ]) {
        if (size <= 1) {
          lemmas[   -1 ] = clause[ size ];
          clause[ size ] = lit; }
        buffer[ size++ ] = lit; } }

    if (clause[1]) {
      addWatch (S, clause, 0);
      addWatch (S, clause, 1); }

    lemmas += EXTRA;

    if (flag     ) adlist[ checked - 1 ] = 0;
    if (flag     ) continue;   // Clause is already satisfied
    if (size == 0) { printf("c conflict claimed, but not detected\n"); return SAT; }

    if (size == 1) {
      ASSIGN (buffer[0]); S->reason[abs(buffer[0])] = ((long) ((clause)-S->DB)) + 1;
      S->forced = S->processed;
      S->RATforced = S->forced;
      if (propagate (S) == UNSAT) goto start_verification; }

    if (((long) (lemmas - S->DB)) >= S->mem_used) break;  // Reached the end of the proof without a conflict;
  }

  printf("c no conflict\n");
  return SAT;

  start_verification:;
  printf("c parsed formula and detected empty clause; start verification\n");

  /* for (int i = 0; i < S->mem_used; i++) { */
  /*   printf("S->DB[%d] = %d\n", i, S->DB[i]); */
  /* } */

  S->forced = S->processed;
  S->RATforced = S->forced;
  lemmas  = clause - EXTRA;

  for (;;) {
    size    = 0;
    clause  = lemmas + EXTRA;

    //printf("Verifying clause:  "); printClause(clause);

    do {
      ad = adlist[ --checked ];
      d = ad & 1;
      int* c = S->DB + (ad >> 1);
      if (d && c[1]) {
        if (S->reason[ abs(c[0]) ] - 1 == (ad >> 1)) continue;
        addWatch(S, c, 0);
        addWatch(S, c, 1); }
    }
    while (d);
    
    S->time = clause[-1];

    if (clause[1]) {                 // if clause is not unit
      removeWatch(S, clause, 0);     // add two watch pointers
      removeWatch(S, clause, 1); }

    if (ad == 0) goto next_lemma;

    while (*clause) {
      int lit = *(clause++);
      if ( S->false[ -lit ]) flag = 1;
      if (!S->false[  lit ])
      buffer[ size++ ] = lit; }

      if (flag && size == 1) {
      do { S->false[*(--S->forced)] = 0; }
      while (*S->forced != -buffer[0]);
      S->processed = S->assigned = S->RATforced = S->forced; }

    if (S->time & 1) {
      int i;

      struct timeval current_time;
      gettimeofday(&current_time, NULL);
      int seconds = (int) (current_time.tv_sec - S->start_time.tv_sec);
      if (seconds > S->timeout) printf("s TIMEOUT\n"), exit(0);

      if (S->traceFile) {
        fprintf(S->traceFile, "%lu ", S->time >> 1);
        for (i = 0; i < size; ++i) fprintf(S->traceFile, "%i ", buffer[i]);
        fprintf(S->traceFile, "0 "); }

      for (i = 0; i < size; ++i) { ASSIGN(-buffer[i]); S->reason[abs(buffer[i])] = 0; }
      if (propagate (S) == SAT) {
	// ===================================================================
	// Failed RUP check.  Now test RAT.
	// printf("RUP check failed.  Starting RAT check.\n");

	// Can keep all assigned literals because any resolution
	// candidate will be forcing resolving literal to false.
        // Resolution literal will be false in new definition.

	S->RATmode = 1;
	S->currentRATdependencies = 0;

	clause = lemmas + EXTRA; // back to beginning of clause
	// printf("Potential RAT clause = ");
	// printClause(clause);

	int resolutionLit = clause[-2];  // get resolution lit
	//printf("Resolution literal %d.\n", resolutionLit);

        int blocked;
        long int reason;
	int numCandidates = 0;

	int* savedForced = S->forced;
	S->RATforced = S->forced;
	S->forced = S->assigned;

	// Loop over all literals
	//printf("Calculating resolution candidates.\n");
	for (int i = -S->nVars; i <= S->nVars; i++) {
	  if (i == 0) continue;
	  // Loop over all watched clauses for literal
	  for (int j = 0; j < S->used[i]; j++) {
	    int watchedClauseOffset = wlist[i][j] >> 1;
	    int* watchedClause = S->DB + watchedClauseOffset;
	    // If watched literal is in first position
	    if (*watchedClause == i) {
	      int flag = 0;
              blocked = 0;
              reason = 0;
	      while (*watchedClause) {
		// If -resolutionLit is found in clause, then candidate
		if (*watchedClause == -resolutionLit) flag = 1;

		// Unless some other literal is already assigned to true
		else if (S->false[-*watchedClause]) {
                  if (blocked == 0 || reason > S->reason[ abs(*watchedClause) ])
                    blocked = *watchedClause, reason = S->reason[ abs(*watchedClause) ];
		}
		watchedClause++;
	      }

              if (blocked != 0 && reason != 0 && flag == 1) {
                analyze (S, S->DB + reason, -1); 
                S->reason[abs(blocked)] = 0;
                // printf("c resolvent is blocked on %i with reason %li\n", blocked, reason );
              }

	      // If resolution candidate, add to list
	      if (blocked == 0 && flag == 1) {
		// printf("c Found candidate: ");
		// printClause(S->DB + watchedClauseOffset);
		// Reallocate if resolutionCandidates full.
		if (numCandidates == S->maxCandidates) {
		  //printf("Reallocating candidates.\n");
		  S->maxCandidates = (S->maxCandidates * 3) >> 1;
		  S->resolutionCandidates = realloc(S->resolutionCandidates,
						 sizeof(int) * S->maxCandidates);
		}
		// Add clause to candidates.
		S->resolutionCandidates[numCandidates] = watchedClauseOffset;
		numCandidates++;
	      }
	    }
	  }
	}

	// Check all candidates for RUP
        // if (numCandidates > 0)
	//   printf("Checking %d candidates for RUP at time %li.\n", numCandidates, S->time);
	for (int i = 0; i < numCandidates; i++) {
	  int* candidate = S->DB + S->resolutionCandidates[i];
	  printf("Candidate: ");
	  printClause(candidate);
	  // Assign literals of candidate
	  while (*candidate) {
	    int currentLit = *candidate;
	    if (currentLit != -resolutionLit) {
	      if (!S->false[currentLit]) {
		ASSIGN(-currentLit);
		S->reason[abs(currentLit)] = 0;
	      }
	    }
	    candidate++;
	  }

	  // RUP check
	  int propagateResult = propagate(S);
	  
	  if (propagateResult == SAT) {
	    printf("Resolvent ***FAILED*** RUP check.\n");
	    return SAT;
	  }
	  else {
	    //printf("Resolvent passed RUP check.\n");
	  }
	}

	S->RATcount++;

	if (S->traceFile) {  // This is quadratic, can be n log n
	  for (int i = 0; i < S->currentRATdependencies; i++) {
	    if (S->RATdependencies[i] != 0) {
	      fprintf(S->traceFile, "%d ", S->RATdependencies[i]);
	      for (int j = i + 1; j < S->currentRATdependencies; j++) {
		if (S->RATdependencies[j] == S->RATdependencies[i]) {
		  S->RATdependencies[j] = 0;
		}
	      }
	    }
	  }
	  fprintf(S->traceFile, "0\n");
	}

	S->forced = savedForced;

	// Clean up after failed RUP check of RAT clause.
	while (S->forced < S->assigned) {
	  S->false[*(S->assigned - 1)] = 0;
	  S->assigned--;
	}
	S->processed = S->RATforced = S->forced;

	S->RATmode = 0;

        // ===================================================================
      }
    } //return SAT; }

    clause = lemmas + EXTRA;

    next_lemma:;

    if (lemmas + EXTRA == last) break;
    while (*(--lemmas)); }

postprocess:;
  int marked, count = 0;
  lemmas = S->DB + S->start;
  /* printf("last = %ld\n", last - S->DB); */
  /* printf("lemmas = %ld\n", lemmas - S->DB); */
  while (lemmas + EXTRA <= last) {
    /* printf("lemmas = %ld  *lemmas = %d\n", lemmas - S->DB, *lemmas); */
    if (*(lemmas + 1) & 1) { count++; /* printf("%d marked clause detected %ld\n", count, lemmas - S->DB); */ }
    while (*lemmas++); }
  printf("c %i of %i clauses in core\n", count, S->nClauses);

  // print the core clauses to coreFile in DIMACS format
  if (S->coreFile) {
    fprintf(S->coreFile, "p cnf %i %i\n", S->nVars, count);
    lemmas = S->DB + S->start;
    while (lemmas + EXTRA <= last) {
      marked = *(lemmas + 1) & 1;  // modified
      lemmas += EXTRA - 1;         // added
      while (*lemmas) {
        if (marked) fprintf(S->coreFile, "%i ", *lemmas);
        lemmas++; }

      if (marked) fprintf(S->coreFile, "0\n");
      lemmas++; }
    fclose(S->coreFile); }

  // print the core lemmas to lemmaFile in DRUP format
  if (S->lemmaFile) S->delinfo -= 2;
  int reslit, lcount = 0; count = 0;
  while (lemmas + EXTRA <= S->DB + S->mem_used - 1) {
    lcount++;
    reslit = *lemmas;
    S->time = *(lemmas + 1);
    marked = *(lemmas + 1) & 1;
    lemmas += EXTRA - 1;
    if (marked) count++;
    if (marked && S->lemmaFile) fprintf(S->lemmaFile, "%i ", reslit);
    while (*lemmas) {
      if (marked && S->lemmaFile && *lemmas != reslit)
        fprintf(S->lemmaFile, "%i ", *lemmas);
      lemmas++; }
    lemmas++;

    if (S->lemmaFile == NULL) continue;
    if (marked) fprintf(S->lemmaFile, "0\n");

    while (*S->delinfo == S->time) {
      clause = S->DB + S->delinfo[1];
      fprintf(S->lemmaFile, "d ");
      while (*clause) fprintf(S->lemmaFile, "%i ", *(clause++));
      fprintf(S->lemmaFile, "0\n");
      S->delinfo -= 2; }
  }
  printf("c %i of %i lemmas in core using %lu resolution steps\n", count, lcount, S->arcs);
  printf("c %d RAT lemmas in core\n", S->RATcount);

  // print the resolution graph to traceFile in trace-check format
  if (S->traceFile) {
    lemmas = S->DB + S->start;
    while (lemmas + EXTRA <= last) {
      marked = *(lemmas + 1) & 1;
      lemmas += EXTRA - 1;
      if (marked) fprintf(S->traceFile, "%i ", lemmas[-1] >> 1);
      while (*lemmas) {
        if (marked) fprintf(S->traceFile, "%i ", *lemmas);
        lemmas++; }
      if (marked) fprintf(S->traceFile, "0 0\n");
      lemmas++; }
    fclose(S->traceFile); }

  return UNSAT; }

unsigned int getHash (int* marks, int mark, int* input, int size) {
  unsigned int i, sum  = 0, prod = 1, xor  = 0;
  for (i = 0; i < size; ++i) {
    prod *= input[i];
    sum  += input[i];
    xor  ^= input[i];
    marks[ input[i] ] = mark; }

  return (1023 * sum + prod ^ (31 * xor)) % BIGINIT; }

long matchClause (struct solver* S, long *clauselist, int listsize, int* marks, int mark, int* input, int size) {
  int i, matchsize;
  for (i = 0; i < listsize; ++i) {
    int *clause = S->DB + clauselist[ i ];
    matchsize = 0;
    while (*clause) {
      if (marks[ *clause ] != mark) goto match_next;
      matchsize++;
      clause++; }

    if (size == matchsize)  {
      long result = clauselist[ i ];
      clauselist[ i ] = clauselist[ --listsize ];
      return result; }

    match_next:;
  }
  printf("c error: could not match deleted clause ");
  for (i = 0; i < size; ++i) printf("%i ", input[i]);
  printf("\ns MATCHING ERROR\n");
  exit(0);
  return 0;
}

int parse (struct solver* S) {
  int tmp;
  int del, mark, *marks;
  long **hashTable;
  int *hashUsed, *hashMax;

  do { tmp = fscanf (S->inputFile, " cnf %i %i \n", &S->nVars, &S->nClauses);  // Read the first line
    if (tmp > 0 && tmp != EOF) break; tmp = fscanf (S->inputFile, "%*s\n"); }  // In case a commment line was found
  while (tmp != 2 && tmp != EOF);                                              // Skip it and read next line
  int nZeros = S->nClauses, n = S->nVars;

  // ANTON: the buffer will be re-used in verify(); main() should free()
  if ((buffer = (int*)malloc(S->nVars * sizeof(int))) == NULL) return ERROR;

  S->mem_used   = 0;                  // The number of integers allocated in the DB

  long size;
  long DBsize = S->mem_used + BIGINIT;
  S->DB = (int*) malloc(DBsize * sizeof(int));
  if (S->DB == NULL) { free(buffer); return ERROR; }

  S->arcs       = 0;
  S->count      = 1;
  S->adsize     = 0;
  S->falseStack = (int*) malloc((n + 1) * sizeof(int)); // Stack of falsified literals -- this pointer is never changed
  S->forced     = S->falseStack;      // Points inside *falseStack at first decision (unforced literal)
  S->processed  = S->falseStack;      // Points inside *falseStack at first unprocessed literal
  S->assigned   = S->falseStack;      // Points inside *falseStack at last unprocessed literal
  S->reason     = (long*) malloc((    n + 1) * sizeof(long)); // Array of clauses
  S->used       = (int *) malloc((2 * n + 1) * sizeof(int )); S->used  += n; // Labels for variables, non-zero means false
  S->max        = (int *) malloc((2 * n + 1) * sizeof(int )); S->max   += n; // Labels for variables, non-zero means false
  S->false      = (int *) malloc((2 * n + 1) * sizeof(int )); S->false += n; // Labels for variables, non-zero means false

  S->RATmode  = 0;
  S->RATcount = 0;

  S->maxCandidates = CANDIDATE_INIT_SIZE;
  S->resolutionCandidates = (int*) malloc(sizeof(int) * S->maxCandidates);
  for (int i = 0; i < S->maxCandidates; i++) S->resolutionCandidates[i] = 0;

  S->maxRATdependencies = RAT_DEPENDENCIES_INIT_SIZE;
  S->RATdependencies = (int*) malloc(sizeof(int) * S->maxRATdependencies);
  for (int i = 0; i < S->maxRATdependencies; i++) S->RATdependencies[i] = 0;


  int i; for (i = 1; i <= n; ++i) { S->reason[i] = 0;
                                    S->falseStack[i] = 0;
				    S->false[i] = S->false[-i] = 0;
                                    S->used [i] = S->used [-i] = 0;
                                    S->max  [i] = S->max  [-i] = INIT; }

  wlist = (long**) malloc (sizeof(long*) * (2*n+1)); wlist += n;

  for (i = 1; i <= n; ++i) { wlist[ i] = (long*) malloc (sizeof(long) * S->max[ i]); wlist[ i][0] = END;
                             wlist[-i] = (long*) malloc (sizeof(long) * S->max[-i]); wlist[-i][0] = END; }

  int admax  = BIGINIT;
  adlist = (long*) malloc(sizeof(long) * admax);

  marks = (int*) malloc (sizeof(int) * (2*n+1)); marks += n;
  for (i = 1; i <= n; i++) marks[i] = marks[-i] = 0;
  mark = 0;

  hashTable = (long**) malloc (sizeof (long*) * BIGINIT);
  hashUsed  = (int * ) malloc (sizeof (int  ) * BIGINIT);
  hashMax   = (int * ) malloc (sizeof (int  ) * BIGINIT);
  for (i = 0; i < BIGINIT; i++) {
    hashUsed [ i ] = 0;
    hashMax  [ i ] = INIT;
    hashTable[ i ] = (long*) malloc (sizeof(long) * hashMax[i]); }

  int fileSwitchFlag = 0;
  size = 0;
  S->start = S->mem_used;
  while (1) {
    int lit = 0; tmp = 0;
    fileSwitchFlag = nZeros <= 0;

    if (size == 0) {
      if (!fileSwitchFlag) tmp = fscanf (S->inputFile, " d  %i ", &lit);
      else tmp = fscanf (S->proofFile, " d  %i ", &lit);
      if (tmp == EOF && !fileSwitchFlag) fileSwitchFlag = 1;
      del = tmp > 0; }

    if (!lit) {
      if (!fileSwitchFlag) tmp = fscanf (S->inputFile, " %i ", &lit);  // Read a literal.
      else tmp = fscanf (S->proofFile, " %i ", &lit);
      if (tmp == EOF && !fileSwitchFlag) fileSwitchFlag = 1; }
    if (tmp == EOF && fileSwitchFlag) break;
    if (abs(lit) > n) { printf("c illegal literal %i due to max var %i\n", lit, n); exit(0); }
    if (!lit) {
      unsigned int hash = getHash (marks, ++mark, buffer, size);
      if (del) {
        if (S->delete) {
          long match = matchClause (S, hashTable[hash], hashUsed[hash], marks, mark, buffer, size);
          hashUsed[hash]--;
          if (S->adsize == admax) { admax *= 1.5;
            adlist = (long*) realloc (adlist, sizeof(long) * admax); }
          adlist[ S->adsize++ ] = (match << 1) + 1; }
        del = 0; size = 0; continue; }

      if (S->mem_used + size + EXTRA > DBsize) {
	DBsize = (DBsize * 3) >> 1;
	S->DB = (int *) realloc(S->DB, DBsize * sizeof(int)); }
      int *clause = &S->DB[S->mem_used + EXTRA - 1];
      if (size != 0) clause[-2] = buffer[0];
      clause[-1] = 2 * S->count; S->count++;
      for (i = 0; i < size; ++i) { clause[ i ] = buffer[ i ]; } clause[ i ] = 0;
      S->mem_used += size + EXTRA;

      if (hashUsed[hash] == hashMax[hash]) { hashMax[hash] *= 1.5;
        hashTable[hash] = (long *) realloc(hashTable[hash], sizeof(long*) * hashMax[hash]); }
      hashTable[ hash ][ hashUsed[hash]++ ] = (long) (clause - S->DB);

      if (S->adsize == admax) { admax *= 1.5;
        adlist = (long*) realloc (adlist, sizeof(long) * admax); }
      adlist[ S->adsize++ ] = ((long) (clause - S->DB)) << 1;

      if (nZeros == S->nClauses) S->base = clause;           // change if ever used
      if (!nZeros) S->lemmas   = (long) (clause - S->DB);    // S->lemmas is no longer pointer
      if (!nZeros) S->adlemmas = S->adsize - 1;

      if (nZeros > 0) {
        if (!size || ((size == 1) && S->false[ clause[0] ])) // Check for empty clause or conflicting unit
          return UNSAT;                                      // If either is found return UNSAT; ANTON: memory leak here
        else if (size == 1) {                                // Check for a new unit
          if (!S->false[ -clause[0] ]) {
            S->reason[abs(clause[0])] = ((long) ((clause)-S->DB)) + 1;
            ASSIGN (clause[0]); } }                          // Directly assign new units
        else { addWatch (S, clause, 0); addWatch (S, clause, 1); }
      }
      else if (!size) break;                                 // Redundant empty clause claimed
      size = 0; --nZeros; }                                  // Reset buffer
    else buffer[ size++ ] = lit; }                           // Add literal to buffer

  S->DB = (int *) realloc(S->DB, S->mem_used * sizeof(int));

  for (i = 0; i < BIGINIT; i++) free(hashTable[i]);
  free(hashTable);
  free(hashUsed);
  free(hashMax);
  free(marks - S->nVars);

  return SAT; }

void freeMemory(struct solver *S) {
  free(S->DB);
  free(S->falseStack);
  free(S->reason);
  free(adlist);
  int i; for (i = 1; i <= S->nVars; ++i) { free(wlist[i]); free(wlist[-i]); }
  free(S->used  - S->nVars);
  free(S->max   - S->nVars);
  free(S->false - S->nVars);
  free(wlist    - S->nVars);
  if (buffer != 0) { free(buffer); }
  free(S->resolutionCandidates);
  free(S->RATdependencies);
  return;
}

int main (int argc, char** argv) {
  struct solver S;

  S.inputFile = NULL;
  S.proofFile = stdin;
  S.coreFile  = NULL;
  S.lemmaFile = NULL;
  S.traceFile = NULL;
  S.timeout   = TIMEOUT;
  S.mask      = 0;
  S.delete    = 1;
  gettimeofday(&S.start_time, NULL);

  int i, tmp = 0;
  for (i = 1; i < argc; i++) {
    if (argv[i][0] == '-') {
      if (argv[i][1] == 'h') {
        printf("usage: drup-trim [INPUT] [<PROOF>] [<option> ...]\n\n");
        printf("where <option> is one of the following\n\n");
        printf("  -h          print this command line option summary\n");
        printf("  -c CORE     prints the unsatisfiable core to the file CORE\n");
        printf("  -l LEMMAS   prints the core lemmas to the file LEMMAS\n");
        printf("  -r TRACE    resolution graph in TRACECHECK format\n\n");
        printf("  -t <lim>    time limit in seconds (default %i)\n", TIMEOUT);
        printf("  -u          default unit propatation (i.e., no core-first)\n");
        printf("  -p          run in plain mode (i.e., ignore deletion information)\n\n");
        printf("and input and proof are specified as follows\n\n");
        printf("  INPUT       input file in DIMACS format\n");
        printf("  PROOF       proof file in DRUP format (stdin if no argument)\n\n");
        exit(0);
      }
      if (argv[i][1] == 'c') S.coreFile  = fopen (argv[++i], "w");
      else if (argv[i][1] == 'l') S.lemmaFile = fopen (argv[++i], "w");
      else if (argv[i][1] == 'r') S.traceFile = fopen (argv[++i], "w");
      else if (argv[i][1] == 't') S.timeout   = atoi(argv[++i]);
      else if (argv[i][1] == 'u') S.mask      = 1;
      else if (argv[i][1] == 'p') S.delete    = 0;
    }
    else {
      tmp++;
      if (tmp == 1) {
        S.inputFile = fopen (argv[1], "r");
        if (S.inputFile == NULL) {
          printf("c error opening \"%s\".\n", argv[i]);
          return 1; } }

      else if (tmp == 2) {
        S.proofFile = fopen (argv[2], "r");
        if (S.proofFile == NULL) {
          printf("c error opening \"%s\".\n", argv[i]);
          return 1; } }
    }
  }
  if (tmp == 1) printf("c reading proof from stdin\n");

  int parseReturnValue = parse(&S);

  fclose (S.inputFile);
  fclose (S.proofFile);
  int sts = ERROR;
  if       (parseReturnValue == ERROR) printf("s MEMORY ALLOCATION ERROR\n");
  else if  (parseReturnValue == UNSAT) printf("s TRIVIAL UNSAT\n");
  else if  ((sts = verify (&S)) == UNSAT) printf("s VERIFIED\n");
  else printf("s NOT VERIFIED\n")  ;
  freeMemory(&S);
  return (sts != UNSAT); // 0 on success, 1 on any failure
}
