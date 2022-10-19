package org.springframework.boot.loader.jar;

interface JarEntryFilter {
   AsciiBytes apply(AsciiBytes name);
}
