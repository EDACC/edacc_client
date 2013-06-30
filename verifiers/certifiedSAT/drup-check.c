// drup-check.c   last edit: May 25, 2013

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/time.h>

//#define COREFIRST

#define TIMEOUT     20000

#define BIGINIT     1000000
#define INIT        8
#define END         0
#define UNSAT       0
#define SAT         1
#define EXTRA       2
#define MARK        3
#define ERROR      -1

struct solver { FILE *file, *proofFile; int *DB, nVars, nClauses, *falseStack, *false, *forced,
    *processed, *assigned, count, *base, *used, *max;
    long mem_used, start, time, adsize, adlemmas, *reason, lemmas, arcs;  };

struct timeval start_time, current_time;

long **wlist;
long *adlist;

#define ASSIGN(a)	{ S->false[-(a)] = 1; *(S->assigned++) = -(a); }
#define ADD_WATCH(l,m)  { if (S->used[(l)] + 1 == S->max[(l)]) { S->max[(l)] *= 1.5; \
                            wlist[(l)] = (long *) realloc(wlist[(l)], sizeof(long) * S->max[(l)]); } \
                          wlist[(l)][ S->used[(l)]++ ] = (m); wlist[(l)][ S->used[(l)] ] = END; }

inline void addWatch (struct solver* S, int* clause, int index) {
  int lit = clause[ index ];
  if (S->used[lit] + 1 == S->max[lit]) { S->max[lit] *= 1.5;
    wlist[lit] = (long*) realloc(wlist[lit], sizeof(long) * S->max[lit]); }
  wlist[lit][ S->used[lit]++ ] = ((long) ((clause) - S->DB)) << 1;
  wlist[lit][ S->used[lit]   ] = END; }

inline void removeWatch (struct solver* S, int* clause, int index) {
  int lit = clause[index]; long *watch = wlist[lit];
  for (;;) {
    int* _clause = S->DB + (*(watch++) >> 1);
    if (_clause == clause) {
      watch[-1] = wlist[lit][ --S->used[lit] ];
      wlist[lit][ S->used[lit] ] = END; return; } } }

inline void markWatch (struct solver* S, int* clause, int index, int offset) {
  long* watch = wlist[ clause[ index ] ];
  for (;;) {
    int *_clause = (S->DB + (*(watch++) >> 1) + (long) offset);
    if (_clause == clause) { watch[-1] |= 1; return; } } }

inline void markClause (struct solver* S, int* clause, int index) {
  S->arcs++;
  if ((clause[index - 1] & 1) == 0) {
    clause[index - 1] |= 1;
    if (clause[1 + index] == 0) return;
    markWatch (S, clause,     index, -index);
    markWatch (S, clause, 1 + index, -index); }
  while (*clause) S->false[ *(clause++) ] = MARK; }

void analyze (struct solver* S, int* clause) {     // Mark all clauses involved in conflict
  markClause (S, clause, 0);
  while (S->assigned > S->falseStack) {
    int lit = *(--S->assigned);
    if ((S->false[ lit ] == MARK) &&
        (S->reason[ abs(lit) ]) )
      markClause (S, S->DB+S->reason[ abs(lit) ], -1);
    S->false[ lit ] = (S->assigned < S->forced); }
  S->processed = S->assigned = S->forced; }

int propagate (struct solver* S) {                 // Performs unit propagation
  int *start[2], check = 0;
  int i, lit, _lit = 0; long *watch, *_watch;
  start[0] = start[1] = S->processed;
  flip_check:;
  check ^= 1;
  while (start[check] < S->assigned) {             // While unprocessed false literals
    lit = *(start[check]++);                       // Get first unprocessed literal
    if (lit == _lit) watch = _watch;
    else watch = wlist[ lit ];                     // Obtain the first watch pointer
    while (*watch != END) {                        // While there are watched clauses (watched by lit)
#ifdef COREFIRST
      if ((*watch & 1) != check) {
        watch++; continue; }
#endif
     int *clause = S->DB + (*watch / 2);	   // Get the clause from DB
     if (S->false[ -clause[0] ] ||
         S->false[ -clause[1] ]) {
       watch++; continue; }
     if (clause[0] == lit) clause[0] = clause[1];  // Ensure that the other watched literal is in front
      for (i = 2; clause[i]; ++i)                  // Scan the non-watched literals
        if (S->false[ clause[i] ] == 0) {          // When clause[j] is not false, it is either true or unset
          clause[1] = clause[i]; clause[i] = lit;  // Swap literals
          ADD_WATCH (clause[1], *watch);           // Add the watch to the list of clause[1]
          *watch = wlist[lit][ --S->used[lit] ];   // Remove pointer
          wlist[lit][ S->used[lit] ] = END;
          goto next_clause; }                      // Goto the next watched clause
      clause[1] = lit; watch++;                    // Set lit at clause[1] and set next watch
      if (!S->false[  clause[0] ]) {               // If the other watched literal is falsified,
        ASSIGN (clause[0]);                        // A unit clause is found, and the reason is set
        S->reason[abs(clause[0])] = ((long) ((clause)-S->DB)) + 1;
        if (!check) {
          start[0]--; _lit = lit; _watch = watch;
          goto flip_check; } }
      else { analyze(S, clause); return UNSAT; }   // Found a root level conflict -> UNSAT
      next_clause: ; } }                           // Set position for next clause
  if (check) goto flip_check;
  S->processed = S->assigned;
  return SAT; }	                                   // Finally, no conflict was found

int verify (struct solver *S) {
  int buffer [S->nVars];
  long ad, d = 0;
  int flag, check, size;
  int *clause;
  int *lemmas = (S->DB + S->lemmas);
  int *last   = lemmas;
  int checked = S->adlemmas;

  S->time = lemmas[-1];
  if (S->processed < S->assigned)
    if (propagate (S) == UNSAT) return UNSAT;
  S->forced = S->processed;

  for (;;) {
    flag = size = 0;
    S->time = lemmas[-1];
    clause  = lemmas;

    do {
      ad = adlist[ checked++ ]; d = ad & 1;
      int* c = S->DB + (ad >> 1);
      if (d && c[1]) {
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
      if (propagate (S) == UNSAT) goto start_verification; }

    if (((long) (lemmas - S->DB)) >= S->mem_used) break;  // Reached the end of the proof without a conflict;
  }

  printf("c no conflict\n");
  return SAT;

  start_verification:;
  int *end = lemmas;
  printf("c parsed formula and detected empty clause; start verification\n");

  S->forced = S->processed;
  lemmas    = clause - EXTRA;

  for (;;) {
    size    = 0;
    clause  = lemmas + EXTRA;

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

    if (clause[1]) {
      removeWatch(S, clause, 0);
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
      S->processed = S->assigned = S->forced; }


    if (S->time & 1) {
#ifdef TIMEOUT
      gettimeofday(&current_time, NULL);
      int seconds = (int) current_time.tv_sec - start_time.tv_sec;
      if (seconds > TIMEOUT) printf("s TIMEOUT\n"), exit(0);
#endif
      int i;
      for (i = 0; i < size; ++i) { ASSIGN(-buffer[i]); S->reason[abs(buffer[i])] = 0; }
      if (propagate (S) == SAT) return SAT; }

    clause = lemmas + EXTRA;

    next_lemma:;

    if (lemmas + EXTRA == last) break;
    while (*(--lemmas)); }

  lemmas = S->DB + S->start;
  int count = 0;
  while (lemmas + EXTRA <= last) {
    if (*(lemmas++) & 1) count++;
    while (*lemmas++); }
  printf("c %i of %i clauses in core\n", count, S->nClauses);

  int lcount = 0; count = 0;
  while (lemmas + EXTRA <= end) {
    lcount++;
    if (*(lemmas++) & 1) count++;
    while (*lemmas++); }
  printf("c %i of %i lemmas in core with %lu incoming arcs\n", count, lcount, S->arcs);

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

  do { tmp = fscanf (S->file, " cnf %i %i \n", &S->nVars, &S->nClauses);    // Read the first line
    if (tmp > 0 && tmp != EOF) break; tmp = fscanf (S->file, "%*s\n"); }    // In case a commment line was found
  while (tmp != 2 && tmp != EOF);                                           // Skip it and read next line
  int nZeros = S->nClauses, buffer [S->nVars], in, out, n = S->nVars; // Make a local buffer

  S->mem_used   = 0;                  // The number of integers allocated in the DB

  long size;
  long DBsize = S->mem_used + BIGINIT;
  S->DB = (int*) malloc(DBsize * sizeof(int));
  if (S->DB == NULL) return ERROR;

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
      if (!fileSwitchFlag) tmp = fscanf (S->file, " d  %i ", &lit);
      else tmp = fscanf (S->proofFile, " d  %i ", &lit);
      if (tmp == EOF && !fileSwitchFlag) fileSwitchFlag = 1;
      del = tmp > 0; }

    if (!lit) {
      if (!fileSwitchFlag) tmp = fscanf (S->file, " %i ", &lit);  // Read a literal.
      else tmp = fscanf (S->proofFile, " %i ", &lit);
      if (tmp == EOF && !fileSwitchFlag) fileSwitchFlag = 1; }
    if (tmp == EOF && fileSwitchFlag) break;
    if (!lit) {
      unsigned int hash = getHash (marks, ++mark, buffer, size);
      if (del) {
        long match = matchClause (S, hashTable[hash], hashUsed[hash], marks, mark, buffer, size);
        hashUsed[hash]--;
        if (S->adsize == admax) { admax *= 1.5;
          adlist = (long*) realloc (adlist, sizeof(long) * admax); }
        adlist[ S->adsize++ ] = (match << 1) + 1;
        del = 0; size = 0; continue; }

      if (S->mem_used + size + EXTRA > DBsize) {
	DBsize = (DBsize * 3) / 2;
	S->DB = (int *) realloc(S->DB, DBsize * sizeof(int)); }
      int *clause = &S->DB[S->mem_used + 1];
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
          return UNSAT;                                      // If either is found return UNSAT
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
  return;
}

int main (int argc, char** argv) {
  struct solver S;

  gettimeofday(&start_time, NULL);
  S.file = fopen (argv[1], "r");
  if (S.file == NULL) {
    printf("c error opening \"%s\".\n", argv[1]);
    return 0; }

  if (argc > 2) {
    S.proofFile = fopen (argv[2], "r");
    if (S.proofFile == NULL) {
      printf("c error opening \"%s\".\n", argv[2]);
      return 0; } }
  else {
    printf("c reading proof from stdin\n");
    S.proofFile = stdin; }

  int parseReturnValue = parse(&S);

  fclose (S.file);
  fclose (S.proofFile);
  if       (parseReturnValue == ERROR) printf("s MEMORY ALLOCATION ERROR\n");
  else if  (parseReturnValue == UNSAT) printf("s TRIVIAL UNSAT\n");
  else if  (verify (&S)      == UNSAT) printf("s VERIFIED\n");
  else                                 printf("s NOT VERIFIED\n")  ;
  freeMemory(&S);
  return 0;
}
