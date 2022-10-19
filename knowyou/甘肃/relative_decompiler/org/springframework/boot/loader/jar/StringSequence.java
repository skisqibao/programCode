package org.springframework.boot.loader.jar;

import java.util.Objects;

final class StringSequence implements CharSequence {
   private final String source;
   private final int start;
   private final int end;
   private int hash;

   StringSequence(String source) {
      this(source, 0, source != null ? source.length() : -1);
   }

   StringSequence(String source, int start, int end) {
      Objects.requireNonNull(source, "Source must not be null");
      if (start < 0) {
         throw new StringIndexOutOfBoundsException(start);
      } else if (end > source.length()) {
         throw new StringIndexOutOfBoundsException(end);
      } else {
         this.source = source;
         this.start = start;
         this.end = end;
      }
   }

   public StringSequence subSequence(int start) {
      return this.subSequence(start, this.length());
   }

   public StringSequence subSequence(int start, int end) {
      int subSequenceStart = this.start + start;
      int subSequenceEnd = this.start + end;
      if (subSequenceStart > this.end) {
         throw new StringIndexOutOfBoundsException(start);
      } else if (subSequenceEnd > this.end) {
         throw new StringIndexOutOfBoundsException(end);
      } else {
         return start == 0 && subSequenceEnd == this.end ? this : new StringSequence(this.source, subSequenceStart, subSequenceEnd);
      }
   }

   public boolean isEmpty() {
      return this.length() == 0;
   }

   public int length() {
      return this.end - this.start;
   }

   public char charAt(int index) {
      return this.source.charAt(this.start + index);
   }

   public int indexOf(char ch) {
      return this.source.indexOf(ch, this.start) - this.start;
   }

   public int indexOf(String str) {
      return this.source.indexOf(str, this.start) - this.start;
   }

   public int indexOf(String str, int fromIndex) {
      return this.source.indexOf(str, this.start + fromIndex) - this.start;
   }

   public boolean startsWith(CharSequence prefix) {
      return this.startsWith(prefix, 0);
   }

   public boolean startsWith(CharSequence prefix, int offset) {
      int prefixLength = prefix.length();
      if (this.length() - prefixLength - offset < 0) {
         return false;
      } else {
         int prefixOffset = 0;
         int var5 = offset;

         do {
            if (prefixLength-- == 0) {
               return true;
            }
         } while(this.charAt(var5++) == prefix.charAt(prefixOffset++));

         return false;
      }
   }

   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      } else if (!(obj instanceof CharSequence)) {
         return false;
      } else {
         CharSequence other = (CharSequence)obj;
         int n = this.length();
         if (n != other.length()) {
            return false;
         } else {
            for(int i = 0; n-- != 0; ++i) {
               if (this.charAt(i) != other.charAt(i)) {
                  return false;
               }
            }

            return true;
         }
      }
   }

   public int hashCode() {
      int hash = this.hash;
      if (hash == 0 && this.length() > 0) {
         for(int i = this.start; i < this.end; ++i) {
            hash = 31 * hash + this.source.charAt(i);
         }

         this.hash = hash;
      }

      return hash;
   }

   public String toString() {
      return this.source.substring(this.start, this.end);
   }
}
