#include <stdio.h>

#include "lzma.h"
#include "lzma/C/LzmaDec.h"
#include "lzma/C/Alloc.h"
#include "lzma/C/7zFile.h"
#include "log.h"

static void *SzAlloc(void *p, size_t size) {
	p = p;
	return MyAlloc(size);
}
static void SzFree(void *p, void *address) {
	p = p;
	MyFree(address);
}
static ISzAlloc g_Alloc = { SzAlloc, SzFree };
static SRes Decode2(CLzmaDec *state, ISeqOutStream *outStream,
		ISeqInStream *inStream, UInt64 unpackSize) {
	int thereIsSize = (unpackSize != (UInt64) (Int64) -1);
	Byte inBuf[IN_BUF_SIZE];
	Byte outBuf[OUT_BUF_SIZE];
	size_t inPos = 0, inSize = 0, outPos = 0;
	LzmaDec_Init(state);
	for (;;) {
		if (inPos == inSize) {
			inSize = IN_BUF_SIZE;
			RINOK(inStream->Read(inStream, inBuf, &inSize));
			inPos = 0;
		}
		{
			SRes res;
			SizeT inProcessed = inSize - inPos;
			SizeT outProcessed = OUT_BUF_SIZE - outPos;
			ELzmaFinishMode finishMode = LZMA_FINISH_ANY;
			ELzmaStatus status;
			if (thereIsSize && outProcessed > unpackSize) {
				outProcessed = (SizeT) unpackSize;
				finishMode = LZMA_FINISH_END;
			}

			res = LzmaDec_DecodeToBuf(state, outBuf + outPos, &outProcessed,
					inBuf + inPos, &inProcessed, finishMode, &status);
			inPos += inProcessed;
			outPos += outProcessed;
			unpackSize -= outProcessed;

			if (outStream)
				if (outStream->Write(outStream, outBuf, outPos) != outPos)
					return SZ_ERROR_WRITE;

			outPos = 0;

			if (res != SZ_OK || thereIsSize && unpackSize == 0)
				return res;

			if (inProcessed == 0 && outProcessed == 0) {
				if (thereIsSize || status != LZMA_STATUS_FINISHED_WITH_MARK)
					return SZ_ERROR_DATA;
				return res;
			}
		}
	}
}

static SRes Decode(ISeqOutStream *outStream, ISeqInStream *inStream) {
	UInt64 unpackSize;
	int i;
	SRes res = 0;

	CLzmaDec state;

	/* header: 5 bytes of LZMA properties and 8 bytes of uncompressed size */
	unsigned char header[LZMA_PROPS_SIZE + 8];

	/* Read and parse header */

	RINOK(SeqInStream_Read(inStream, header, sizeof(header)));

	unpackSize = 0;
	for (i = 0; i < 8; i++)
		unpackSize += (UInt64) header[LZMA_PROPS_SIZE + i] << (i * 8);

	LzmaDec_Construct(&state);
	RINOK(LzmaDec_Allocate(&state, header, LZMA_PROPS_SIZE, &g_Alloc));
	res = Decode2(&state, outStream, inStream, unpackSize);
	LzmaDec_Free(&state, &g_Alloc);
	return res;
}

int is_lzma(string filename) {
	FILE* fp = fopen(filename.c_str(), "rb");
	char buf[5];
	fgets(buf, 5, fp);
	fclose(fp);
	return buf[0] == 'L' && (buf[1] == 'Z') && (buf[2] == 'M') && (buf[3] == 'A');
}

int lzma_extract(string filename_source, string filename_dest) {
	CFileSeqInStream inStream;
	CFileOutStream outStream;

	FileSeqInStream_CreateVTable(&inStream);
	File_Construct(&inStream.file);

	FileOutStream_CreateVTable(&outStream);
	File_Construct(&outStream.file);

	if (InFile_Open(&inStream.file, filename_source.c_str()) != 0) {
		log_error(AT, "Cannot open input file");
		return 0;
	}
	if (OutFile_Open(&outStream.file, filename_dest.c_str()) != 0) {
		log_error(AT, "Cannot open output file");
		return 0;
	}
	char buf[5];
	SeqInStream_Read(&inStream.s, buf, 4);
	int res = Decode(&outStream.s, &inStream.s);
	File_Close(&outStream.file);
	File_Close(&inStream.file);
	return res == SZ_OK;
}

