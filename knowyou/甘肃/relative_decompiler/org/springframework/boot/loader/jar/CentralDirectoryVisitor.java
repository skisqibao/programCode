package org.springframework.boot.loader.jar;

import org.springframework.boot.loader.data.RandomAccessData;

interface CentralDirectoryVisitor {
   void visitStart(CentralDirectoryEndRecord endRecord, RandomAccessData centralDirectoryData);

   void visitFileHeader(CentralDirectoryFileHeader fileHeader, int dataOffset);

   void visitEnd();
}
