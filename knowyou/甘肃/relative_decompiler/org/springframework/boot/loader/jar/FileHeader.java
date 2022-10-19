package org.springframework.boot.loader.jar;

interface FileHeader {
   boolean hasName(CharSequence name, char suffix);

   long getLocalHeaderOffset();

   long getCompressedSize();

   long getSize();

   int getMethod();
}
