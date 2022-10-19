package org.springframework.boot.loader.data;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class RandomAccessDataFile implements RandomAccessData {
   private final RandomAccessDataFile.FileAccess fileAccess;
   private final long offset;
   private final long length;

   public RandomAccessDataFile(File file) {
      if (file == null) {
         throw new IllegalArgumentException("File must not be null");
      } else {
         this.fileAccess = new RandomAccessDataFile.FileAccess(file);
         this.offset = 0L;
         this.length = file.length();
      }
   }

   private RandomAccessDataFile(RandomAccessDataFile.FileAccess fileAccess, long offset, long length) {
      this.fileAccess = fileAccess;
      this.offset = offset;
      this.length = length;
   }

   public File getFile() {
      return this.fileAccess.file;
   }

   public InputStream getInputStream() throws IOException {
      return new RandomAccessDataFile.DataInputStream();
   }

   public RandomAccessData getSubsection(long offset, long length) {
      if (offset >= 0L && length >= 0L && offset + length <= this.length) {
         return new RandomAccessDataFile(this.fileAccess, this.offset + offset, length);
      } else {
         throw new IndexOutOfBoundsException();
      }
   }

   public byte[] read() throws IOException {
      return this.read(0L, this.length);
   }

   public byte[] read(long offset, long length) throws IOException {
      if (offset > this.length) {
         throw new IndexOutOfBoundsException();
      } else if (offset + length > this.length) {
         throw new EOFException();
      } else {
         byte[] bytes = new byte[(int)length];
         this.read(bytes, offset, 0, bytes.length);
         return bytes;
      }
   }

   private int readByte(long position) throws IOException {
      return position >= this.length ? -1 : this.fileAccess.readByte(this.offset + position);
   }

   private int read(byte[] bytes, long position, int offset, int length) throws IOException {
      return position > this.length ? -1 : this.fileAccess.read(bytes, this.offset + position, offset, length);
   }

   public long getSize() {
      return this.length;
   }

   public void close() throws IOException {
      this.fileAccess.close();
   }

   private static final class FileAccess {
      private final Object monitor;
      private final File file;
      private RandomAccessFile randomAccessFile;

      private FileAccess(File file) {
         this.monitor = new Object();
         this.file = file;
         this.openIfNecessary();
      }

      private int read(byte[] bytes, long position, int offset, int length) throws IOException {
         synchronized(this.monitor) {
            this.openIfNecessary();
            this.randomAccessFile.seek(position);
            return this.randomAccessFile.read(bytes, offset, length);
         }
      }

      private void openIfNecessary() {
         if (this.randomAccessFile == null) {
            try {
               this.randomAccessFile = new RandomAccessFile(this.file, "r");
            } catch (FileNotFoundException var2) {
               throw new IllegalArgumentException(String.format("File %s must exist", this.file.getAbsolutePath()));
            }
         }

      }

      private void close() throws IOException {
         synchronized(this.monitor) {
            if (this.randomAccessFile != null) {
               this.randomAccessFile.close();
               this.randomAccessFile = null;
            }

         }
      }

      private int readByte(long position) throws IOException {
         synchronized(this.monitor) {
            this.openIfNecessary();
            this.randomAccessFile.seek(position);
            return this.randomAccessFile.read();
         }
      }

      // $FF: synthetic method
      FileAccess(File x0, Object x1) {
         this(x0);
      }
   }

   private class DataInputStream extends InputStream {
      private int position;

      private DataInputStream() {
      }

      public int read() throws IOException {
         int read = RandomAccessDataFile.this.readByte((long)this.position);
         if (read > -1) {
            this.moveOn(1);
         }

         return read;
      }

      public int read(byte[] b) throws IOException {
         return this.read(b, 0, b != null ? b.length : 0);
      }

      public int read(byte[] b, int off, int len) throws IOException {
         if (b == null) {
            throw new NullPointerException("Bytes must not be null");
         } else {
            return this.doRead(b, off, len);
         }
      }

      public int doRead(byte[] b, int off, int len) throws IOException {
         if (len == 0) {
            return 0;
         } else {
            int cappedLen = this.cap((long)len);
            return cappedLen <= 0 ? -1 : (int)this.moveOn(RandomAccessDataFile.this.read(b, (long)this.position, off, cappedLen));
         }
      }

      public long skip(long n) throws IOException {
         return n <= 0L ? 0L : this.moveOn(this.cap(n));
      }

      private int cap(long n) {
         return (int)Math.min(RandomAccessDataFile.this.length - (long)this.position, n);
      }

      private long moveOn(int amount) {
         this.position += amount;
         return (long)amount;
      }

      // $FF: synthetic method
      DataInputStream(Object x1) {
         this();
      }
   }
}
