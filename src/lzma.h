#ifndef __lzma_h__
#define __lzma_h__
#include <string>

using std::string;

#define IN_BUF_SIZE (1 << 16)
#define OUT_BUF_SIZE (1 << 16)

int is_lzma(string filename);
int lzma_extract(string filename_source, string filename_dest);
#endif
