package org.springframework.boot.loader.jar;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import org.springframework.boot.loader.data.RandomAccessData;
import org.springframework.boot.loader.data.RandomAccessDataFile;

public class JarFile extends java.util.jar.JarFile {
   private static final String MANIFEST_NAME = "META-INF/MANIFEST.MF";
   private static final String PROTOCOL_HANDLER = "java.protocol.handler.pkgs";
   private static final String HANDLERS_PACKAGE = "org.springframework.boot.loader";
   private static final AsciiBytes META_INF = new AsciiBytes("META-INF/");
   private static final AsciiBytes SIGNATURE_FILE_EXTENSION = new AsciiBytes(".SF");
   private final RandomAccessDataFile rootFile;
   private final String pathFromRoot;
   private final RandomAccessData data;
   private final JarFile.JarFileType type;
   private URL url;
   private String urlString;
   private JarFileEntries entries;
   private Supplier<Manifest> manifestSupplier;
   private SoftReference<Manifest> manifest;
   private boolean signed;

   public JarFile(File file) throws IOException {
      this(new RandomAccessDataFile(file));
   }

   JarFile(RandomAccessDataFile file) throws IOException {
      this(file, "", file, JarFile.JarFileType.DIRECT);
   }

   private JarFile(RandomAccessDataFile rootFile, String pathFromRoot, RandomAccessData data, JarFile.JarFileType type) throws IOException {
      this(rootFile, pathFromRoot, data, (JarEntryFilter)null, type, (Supplier)null);
   }

   private JarFile(RandomAccessDataFile rootFile, String pathFromRoot, RandomAccessData data, JarEntryFilter filter, JarFile.JarFileType type, Supplier<Manifest> manifestSupplier) throws IOException {
      super(rootFile.getFile());
      this.rootFile = rootFile;
      this.pathFromRoot = pathFromRoot;
      CentralDirectoryParser parser = new CentralDirectoryParser();
      this.entries = (JarFileEntries)parser.addVisitor(new JarFileEntries(this, filter));
      parser.addVisitor(this.centralDirectoryVisitor());
      this.data = parser.parse(data, filter == null);
      this.type = type;
      this.manifestSupplier = manifestSupplier != null ? manifestSupplier : () -> {
         try {
            InputStream inputStream = this.getInputStream("META-INF/MANIFEST.MF");
            Throwable var2 = null;

            Manifest var3;
            try {
               if (inputStream != null) {
                  var3 = new Manifest(inputStream);
                  return var3;
               }

               var3 = null;
            } catch (Throwable var14) {
               var2 = var14;
               throw var14;
            } finally {
               if (inputStream != null) {
                  if (var2 != null) {
                     try {
                        inputStream.close();
                     } catch (Throwable var13) {
                        var2.addSuppressed(var13);
                     }
                  } else {
                     inputStream.close();
                  }
               }

            }

            return var3;
         } catch (IOException var16) {
            throw new RuntimeException(var16);
         }
      };
   }

   private CentralDirectoryVisitor centralDirectoryVisitor() {
      return new CentralDirectoryVisitor() {
         public void visitStart(CentralDirectoryEndRecord endRecord, RandomAccessData centralDirectoryData) {
         }

         public void visitFileHeader(CentralDirectoryFileHeader fileHeader, int dataOffset) {
            AsciiBytes name = fileHeader.getName();
            if (name.startsWith(JarFile.META_INF) && name.endsWith(JarFile.SIGNATURE_FILE_EXTENSION)) {
               JarFile.this.signed = true;
            }

         }

         public void visitEnd() {
         }
      };
   }

   protected final RandomAccessDataFile getRootJarFile() {
      return this.rootFile;
   }

   RandomAccessData getData() {
      return this.data;
   }

   public Manifest getManifest() throws IOException {
      Manifest manifest = this.manifest != null ? (Manifest)this.manifest.get() : null;
      if (manifest == null) {
         try {
            manifest = (Manifest)this.manifestSupplier.get();
         } catch (RuntimeException var3) {
            throw new IOException(var3);
         }

         this.manifest = new SoftReference(manifest);
      }

      return manifest;
   }

   public Enumeration<java.util.jar.JarEntry> entries() {
      final Iterator<JarEntry> iterator = this.entries.iterator();
      return new Enumeration<java.util.jar.JarEntry>() {
         public boolean hasMoreElements() {
            return iterator.hasNext();
         }

         public java.util.jar.JarEntry nextElement() {
            return (java.util.jar.JarEntry)iterator.next();
         }
      };
   }

   public JarEntry getJarEntry(CharSequence name) {
      return this.entries.getEntry(name);
   }

   public JarEntry getJarEntry(String name) {
      return (JarEntry)this.getEntry(name);
   }

   public boolean containsEntry(String name) {
      return this.entries.containsEntry(name);
   }

   public ZipEntry getEntry(String name) {
      return this.entries.getEntry(name);
   }

   public synchronized InputStream getInputStream(ZipEntry entry) throws IOException {
      return entry instanceof JarEntry ? this.entries.getInputStream((FileHeader)((JarEntry)entry)) : this.getInputStream(entry != null ? entry.getName() : null);
   }

   InputStream getInputStream(String name) throws IOException {
      return this.entries.getInputStream(name);
   }

   public synchronized JarFile getNestedJarFile(ZipEntry entry) throws IOException {
      return this.getNestedJarFile((JarEntry)entry);
   }

   public synchronized JarFile getNestedJarFile(JarEntry entry) throws IOException {
      try {
         return this.createJarFileFromEntry(entry);
      } catch (Exception var3) {
         throw new IOException("Unable to open nested jar file '" + entry.getName() + "'", var3);
      }
   }

   private JarFile createJarFileFromEntry(JarEntry entry) throws IOException {
      return entry.isDirectory() ? this.createJarFileFromDirectoryEntry(entry) : this.createJarFileFromFileEntry(entry);
   }

   private JarFile createJarFileFromDirectoryEntry(JarEntry entry) throws IOException {
      AsciiBytes name = entry.getAsciiBytesName();
      JarEntryFilter filter = (candidate) -> {
         return candidate.startsWith(name) && !candidate.equals(name) ? candidate.substring(name.length()) : null;
      };
      return new JarFile(this.rootFile, this.pathFromRoot + "!/" + entry.getName().substring(0, name.length() - 1), this.data, filter, JarFile.JarFileType.NESTED_DIRECTORY, this.manifestSupplier);
   }

   private JarFile createJarFileFromFileEntry(JarEntry entry) throws IOException {
      if (entry.getMethod() != 0) {
         throw new IllegalStateException("Unable to open nested entry '" + entry.getName() + "'. It has been compressed and nested jar files must be stored without compression. Please check the mechanism used to create your executable jar file");
      } else {
         RandomAccessData entryData = this.entries.getEntryData(entry.getName());
         return new JarFile(this.rootFile, this.pathFromRoot + "!/" + entry.getName(), entryData, JarFile.JarFileType.NESTED_JAR);
      }
   }

   public int size() {
      return this.entries.getSize();
   }

   public void close() throws IOException {
      super.close();
      if (this.type == JarFile.JarFileType.DIRECT) {
         this.rootFile.close();
      }

   }

   String getUrlString() throws MalformedURLException {
      if (this.urlString == null) {
         this.urlString = this.getUrl().toString();
      }

      return this.urlString;
   }

   public URL getUrl() throws MalformedURLException {
      if (this.url == null) {
         Handler handler = new Handler(this);
         String file = this.rootFile.getFile().toURI() + this.pathFromRoot + "!/";
         file = file.replace("file:////", "file://");
         this.url = new URL("jar", "", -1, file, handler);
      }

      return this.url;
   }

   public String toString() {
      return this.getName();
   }

   public String getName() {
      return this.rootFile.getFile() + this.pathFromRoot;
   }

   boolean isSigned() {
      return this.signed;
   }

   void setupEntryCertificates(JarEntry entry) {
      try {
         JarInputStream inputStream = new JarInputStream(this.getData().getInputStream());
         Throwable var3 = null;

         try {
            for(java.util.jar.JarEntry certEntry = inputStream.getNextJarEntry(); certEntry != null; certEntry = inputStream.getNextJarEntry()) {
               inputStream.closeEntry();
               if (entry.getName().equals(certEntry.getName())) {
                  this.setCertificates(entry, certEntry);
               }

               this.setCertificates(this.getJarEntry(certEntry.getName()), certEntry);
            }
         } catch (Throwable var13) {
            var3 = var13;
            throw var13;
         } finally {
            if (inputStream != null) {
               if (var3 != null) {
                  try {
                     inputStream.close();
                  } catch (Throwable var12) {
                     var3.addSuppressed(var12);
                  }
               } else {
                  inputStream.close();
               }
            }

         }

      } catch (IOException var15) {
         throw new IllegalStateException(var15);
      }
   }

   private void setCertificates(JarEntry entry, java.util.jar.JarEntry certEntry) {
      if (entry != null) {
         entry.setCertificates(certEntry);
      }

   }

   public void clearCache() {
      this.entries.clearCache();
   }

   protected String getPathFromRoot() {
      return this.pathFromRoot;
   }

   JarFile.JarFileType getType() {
      return this.type;
   }

   public static void registerUrlProtocolHandler() {
      String handlers = System.getProperty("java.protocol.handler.pkgs", "");
      System.setProperty("java.protocol.handler.pkgs", "".equals(handlers) ? "org.springframework.boot.loader" : handlers + "|" + "org.springframework.boot.loader");
      resetCachedUrlHandlers();
   }

   private static void resetCachedUrlHandlers() {
      try {
         URL.setURLStreamHandlerFactory((URLStreamHandlerFactory)null);
      } catch (Error var1) {
      }

   }

   static enum JarFileType {
      DIRECT,
      NESTED_DIRECTORY,
      NESTED_JAR;
   }
}
