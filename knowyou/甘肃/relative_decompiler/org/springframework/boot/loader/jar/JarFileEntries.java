package org.springframework.boot.loader.jar;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.jar.Attributes.Name;
import org.springframework.boot.loader.data.RandomAccessData;

class JarFileEntries implements CentralDirectoryVisitor, Iterable<JarEntry> {
   private static final String META_INF_PREFIX = "META-INF/";
   private static final Name MULTI_RELEASE = new Name("Multi-Release");
   private static final int BASE_VERSION = 8;
   private static final int RUNTIME_VERSION;
   private static final long LOCAL_FILE_HEADER_SIZE = 30L;
   private static final char SLASH = '/';
   private static final char NO_SUFFIX = '\u0000';
   protected static final int ENTRY_CACHE_SIZE = 25;
   private final JarFile jarFile;
   private final JarEntryFilter filter;
   private RandomAccessData centralDirectoryData;
   private int size;
   private int[] hashCodes;
   private int[] centralDirectoryOffsets;
   private int[] positions;
   private Boolean multiReleaseJar;
   private final Map<Integer, FileHeader> entriesCache = Collections.synchronizedMap(new LinkedHashMap<Integer, FileHeader>(16, 0.75F, true) {
      protected boolean removeEldestEntry(Entry<Integer, FileHeader> eldest) {
         if (JarFileEntries.this.jarFile.isSigned()) {
            return false;
         } else {
            return this.size() >= 25;
         }
      }
   });

   JarFileEntries(JarFile jarFile, JarEntryFilter filter) {
      this.jarFile = jarFile;
      this.filter = filter;
      if (RUNTIME_VERSION == 8) {
         this.multiReleaseJar = false;
      }

   }

   public void visitStart(CentralDirectoryEndRecord endRecord, RandomAccessData centralDirectoryData) {
      int maxSize = endRecord.getNumberOfRecords();
      this.centralDirectoryData = centralDirectoryData;
      this.hashCodes = new int[maxSize];
      this.centralDirectoryOffsets = new int[maxSize];
      this.positions = new int[maxSize];
   }

   public void visitFileHeader(CentralDirectoryFileHeader fileHeader, int dataOffset) {
      AsciiBytes name = this.applyFilter(fileHeader.getName());
      if (name != null) {
         this.add(name, dataOffset);
      }

   }

   private void add(AsciiBytes name, int dataOffset) {
      this.hashCodes[this.size] = name.hashCode();
      this.centralDirectoryOffsets[this.size] = dataOffset;
      this.positions[this.size] = this.size++;
   }

   public void visitEnd() {
      this.sort(0, this.size - 1);
      int[] positions = this.positions;
      this.positions = new int[positions.length];

      for(int i = 0; i < this.size; this.positions[positions[i]] = i++) {
      }

   }

   int getSize() {
      return this.size;
   }

   private void sort(int left, int right) {
      if (left < right) {
         int pivot = this.hashCodes[left + (right - left) / 2];
         int i = left;
         int j = right;

         while(i <= j) {
            while(this.hashCodes[i] < pivot) {
               ++i;
            }

            while(this.hashCodes[j] > pivot) {
               --j;
            }

            if (i <= j) {
               this.swap(i, j);
               ++i;
               --j;
            }
         }

         if (left < j) {
            this.sort(left, j);
         }

         if (right > i) {
            this.sort(i, right);
         }
      }

   }

   private void swap(int i, int j) {
      this.swap(this.hashCodes, i, j);
      this.swap(this.centralDirectoryOffsets, i, j);
      this.swap(this.positions, i, j);
   }

   private void swap(int[] array, int i, int j) {
      int temp = array[i];
      array[i] = array[j];
      array[j] = temp;
   }

   public Iterator<JarEntry> iterator() {
      return new JarFileEntries.EntryIterator();
   }

   public boolean containsEntry(CharSequence name) {
      return this.getEntry(name, FileHeader.class, true) != null;
   }

   public JarEntry getEntry(CharSequence name) {
      return (JarEntry)this.getEntry(name, JarEntry.class, true);
   }

   public InputStream getInputStream(String name) throws IOException {
      FileHeader entry = this.getEntry(name, FileHeader.class, false);
      return this.getInputStream(entry);
   }

   public InputStream getInputStream(FileHeader entry) throws IOException {
      if (entry == null) {
         return null;
      } else {
         InputStream inputStream = this.getEntryData(entry).getInputStream();
         if (entry.getMethod() == 8) {
            inputStream = new ZipInflaterInputStream((InputStream)inputStream, (int)entry.getSize());
         }

         return (InputStream)inputStream;
      }
   }

   public RandomAccessData getEntryData(String name) throws IOException {
      FileHeader entry = this.getEntry(name, FileHeader.class, false);
      return entry == null ? null : this.getEntryData(entry);
   }

   private RandomAccessData getEntryData(FileHeader entry) throws IOException {
      RandomAccessData data = this.jarFile.getData();
      byte[] localHeader = data.read(entry.getLocalHeaderOffset(), 30L);
      long nameLength = Bytes.littleEndianValue(localHeader, 26, 2);
      long extraLength = Bytes.littleEndianValue(localHeader, 28, 2);
      return data.getSubsection(entry.getLocalHeaderOffset() + 30L + nameLength + extraLength, entry.getCompressedSize());
   }

   private <T extends FileHeader> T getEntry(CharSequence name, Class<T> type, boolean cacheEntry) {
      T entry = this.doGetEntry(name, type, cacheEntry, (AsciiBytes)null);
      if (!this.isMetaInfEntry(name) && this.isMultiReleaseJar()) {
         int version = RUNTIME_VERSION;

         for(AsciiBytes nameAlias = entry instanceof JarEntry ? ((JarEntry)entry).getAsciiBytesName() : new AsciiBytes(name.toString()); version > 8; --version) {
            T versionedEntry = this.doGetEntry("META-INF/versions/" + version + "/" + name, type, cacheEntry, nameAlias);
            if (versionedEntry != null) {
               return versionedEntry;
            }
         }
      }

      return entry;
   }

   private boolean isMetaInfEntry(CharSequence name) {
      return name.toString().startsWith("META-INF/");
   }

   private boolean isMultiReleaseJar() {
      Boolean multiRelease = this.multiReleaseJar;
      if (multiRelease != null) {
         return multiRelease;
      } else {
         try {
            Manifest manifest = this.jarFile.getManifest();
            if (manifest == null) {
               multiRelease = false;
            } else {
               Attributes attributes = manifest.getMainAttributes();
               multiRelease = attributes.containsKey(MULTI_RELEASE);
            }
         } catch (IOException var4) {
            multiRelease = false;
         }

         this.multiReleaseJar = multiRelease;
         return multiRelease;
      }
   }

   private <T extends FileHeader> T doGetEntry(CharSequence name, Class<T> type, boolean cacheEntry, AsciiBytes nameAlias) {
      int hashCode = AsciiBytes.hashCode(name);
      T entry = this.getEntry(hashCode, name, '\u0000', type, cacheEntry, nameAlias);
      if (entry == null) {
         hashCode = AsciiBytes.hashCode(hashCode, '/');
         entry = this.getEntry(hashCode, name, '/', type, cacheEntry, nameAlias);
      }

      return entry;
   }

   private <T extends FileHeader> T getEntry(int hashCode, CharSequence name, char suffix, Class<T> type, boolean cacheEntry, AsciiBytes nameAlias) {
      for(int index = this.getFirstIndex(hashCode); index >= 0 && index < this.size && this.hashCodes[index] == hashCode; ++index) {
         T entry = this.getEntry(index, type, cacheEntry, nameAlias);
         if (entry.hasName((CharSequence)(nameAlias != null ? nameAlias.toString() : name), suffix)) {
            return entry;
         }
      }

      return null;
   }

   private <T extends FileHeader> T getEntry(int index, Class<T> type, boolean cacheEntry, AsciiBytes nameAlias) {
      try {
         FileHeader cached = (FileHeader)this.entriesCache.get(index);
         FileHeader entry = cached != null ? cached : CentralDirectoryFileHeader.fromRandomAccessData(this.centralDirectoryData, this.centralDirectoryOffsets[index], this.filter);
         if (CentralDirectoryFileHeader.class.equals(entry.getClass()) && type.equals(JarEntry.class)) {
            entry = new JarEntry(this.jarFile, (CentralDirectoryFileHeader)entry, nameAlias);
         }

         if (cacheEntry && cached != entry) {
            this.entriesCache.put(index, entry);
         }

         return (FileHeader)entry;
      } catch (IOException var7) {
         throw new IllegalStateException(var7);
      }
   }

   private int getFirstIndex(int hashCode) {
      int index = Arrays.binarySearch(this.hashCodes, 0, this.size, hashCode);
      if (index < 0) {
         return -1;
      } else {
         while(index > 0 && this.hashCodes[index - 1] == hashCode) {
            --index;
         }

         return index;
      }
   }

   public void clearCache() {
      this.entriesCache.clear();
   }

   private AsciiBytes applyFilter(AsciiBytes name) {
      return this.filter != null ? this.filter.apply(name) : name;
   }

   static {
      int version;
      try {
         Object runtimeVersion = Runtime.class.getMethod("version").invoke((Object)null);
         version = (Integer)runtimeVersion.getClass().getMethod("major").invoke(runtimeVersion);
      } catch (Throwable var2) {
         version = 8;
      }

      RUNTIME_VERSION = version;
   }

   private class EntryIterator implements Iterator<JarEntry> {
      private int index;

      private EntryIterator() {
         this.index = 0;
      }

      public boolean hasNext() {
         return this.index < JarFileEntries.this.size;
      }

      public JarEntry next() {
         if (!this.hasNext()) {
            throw new NoSuchElementException();
         } else {
            int entryIndex = JarFileEntries.this.positions[this.index];
            ++this.index;
            return (JarEntry)JarFileEntries.this.getEntry(entryIndex, JarEntry.class, false, (AsciiBytes)null);
         }
      }

      // $FF: synthetic method
      EntryIterator(Object x1) {
         this();
      }
   }
}
