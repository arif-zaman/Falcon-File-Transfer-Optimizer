package transfer_protocol.util;


import condor.classad.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;


// This class is a simple wrapper around the more low-level Condor
// ClassAd package. The intent is to make ClassAds less cumbersome to use.

public class ClassAd implements Iterable<String> {
  // This is kind of a hacky thing. These ads are returned by parse when
  // the end of the stream has been reached or an error has occurred.
  private static final ClassAd
          EOF = new ResponseAd("error", "end of stream has been reached"),
          ERROR = new ResponseAd("error", "error parsing ClassAd");

  static {
    EOF.error = true;
    ERROR.error = true;
  }

  private RecordExpr record;
  private boolean error = false;

  // ------------
  // Create a new ClassAd that is a clone of the passed ClassAd.
  public ClassAd(ClassAd ad) {
    this();

    for (String s : ad)
      insert(s, ad.getExpr(s));
  }

  public ClassAd(RecordExpr re) {
    record = re;
  }

  // Access methods
  // --------------
  // Each accessor can optionally have a default value passed as a second
  // argument to be returned if no entry with the given name exists.

  public ClassAd() {
    record = new RecordExpr();
  }

  // Static methods
  // --------------
  // Parse a ClassAd from a string. Returns null if can't parse.
  private static ClassAd parse(String s) {
    try {
      ClassAdParser parser = new ClassAdParser(s);
      Expr expr = parser.parse();

      if (expr instanceof RecordExpr) {
        return new ClassAd((RecordExpr) expr);
      }
      return ERROR;
    } catch (Exception e) {
      return ERROR;
    }
  }

  // Parse a ClassAd from an InputStream. Consumes whitespace until a
  // non-whitespace character is found. Returns null if it can't read or
  // the ad can't be parsed.
  public static ClassAd parse(InputStream is) {
    try {
      boolean escape = false;
      boolean in_str = false;
      int c;
      StringBuilder sb = new StringBuilder(200);

      // Consume whitespaces
      do {
        c = is.read();
      } while (Character.isWhitespace(c));

      // Check if we've reached the end of the stream.
      if (c == -1) {
        return EOF;
      }

      // Check that we're at the beginning of a ClassAd.
      if (c != '[') {
        return ERROR;
      }
      sb.append('[');

      // Read until we get to the end of a ClassAd. Ignore ]'s in strings.
      while (true) {
        c = is.read();
        sb.append((char) c);

        if (c == -1) {
          return EOF;
        }

        if (in_str) {  // Inside of string, ignore ]'s and escaped quotes.
          if (escape) {
            escape = false;  // Still in string if escaped.
          } else if (c == '\\') {
            escape = true;
          } else if (c == '"') {
            in_str = false;
          }
        } else {  // Not in string, look for string or end ].
          if (c == '"') {
            in_str = true;
          } else if (c == ']') {
            break;
          }
        }
      }

      // Try to parse the ad.
      return parse(sb.toString());
    } catch (IOException ioe) {
      return EOF;
    } catch (Exception e) {
      return ERROR;
    }
  }

  // Check if the ClassAd has all of given entries.
  boolean has(String... keys) {
    return require(keys) == null;
  }

  // Get an entry from the ad as a string. Default: null
  public String get(String s) {
    return get(s, null);
  }

  public String get(String s, String def) {
    Expr e = getExpr(s);

    if (e == null) {
      return def;
    }
    if (e.type == Expr.STRING) {
      return e.stringValue();
    } else {
      return e.toString();
    }
  }

  // Get an entry from the ad as an integer. Defaults to -1.
  public int getInt(String s) {
    return getInt(s, -1);
  }

  private int getInt(String s, int def) {
    Expr e = getExpr(s);

    if (e != null) {
      switch (e.type) {
        case Expr.INTEGER:
          return e.intValue();
        case Expr.REAL:
          return (int) e.realValue();
        case Expr.STRING:
          return Integer.parseInt(e.stringValue());
        default:
          return Integer.parseInt(e.toString());
      }
    }
    return def;
  }

  // Get an entry from the ad as a double. Defaults to NaN on error.
  public double getDouble(String s) {
    return getDouble(s, Double.NaN);
  }

  private double getDouble(String s, double def) {
    Expr e = getExpr(s);

    if (e != null) {
      switch (e.type) {
        case Expr.REAL:
        case Expr.INTEGER:
          return e.realValue();
        case Expr.STRING:
          return Double.parseDouble(e.stringValue());
        default:
          return Double.parseDouble(e.toString());
      }
    }
    return def;
  }

  // Get an entry from the ad as a boolean value. Returns true if the value
  // of the entry is "true" (case insensitive), false otherwise.
  public boolean getBoolean(String s) {
    return getBoolean(s, false);
  }

  private boolean getBoolean(String s, boolean def) {
    Expr e = getExpr(s);
    return (e != null) ? e.isTrue() : def;
  }

  // Get an entry as a Condor ClassAd Expr in case someone wants that.
  // Returns null if nothing is found.
  private Expr getExpr(String s) {
    return record.lookup(s);
  }

  // Methods for adding/removing entries
  // -----------------------------------
  // Add new entries to the ClassAd. Trim input. Return this ClassAd.
  public ClassAd insert(String k, String v) {
    insert(k, Constant.getInstance(v));
    return this;
  }

  public ClassAd insert(String k, int v) {
    insert(k, Constant.getInstance(v));
    return this;
  }

  public ClassAd insert(String k, double v) {
    insert(k, Constant.getInstance(v));
    return this;
  }

  private ClassAd insert(String k, Expr v) {
    if (v != null) {
      record.insertAttribute(k, v);
    } else {
      record.removeAttribute(AttrName.fromString(k));
    }
    return this;
  }

  // Delete an entry from this ClassAd. Return this ClassAd.
  public ClassAd remove(String... keys) {
    for (String k : keys)
      record.removeAttribute(AttrName.fromString(k));
    return this;
  }

  // Other methods
  // -------------
  // Get the number of attributes in this ClassAd.
  public int size() {
    return record.size();
  }

  // Apply a filter to this ad, returning a new filtered ad.
  ClassAd filter(String... keys) {
    ClassAd new_ad = new ClassAd();

    if (keys != null) {
      for (String k : keys)
        new_ad.insert(k, getExpr(k));
    }
    return new_ad;
  }

  // Check for required fields, returning the name of the missing
  // field, or null otherwise.
  String require(String... reqs) {
    if (reqs != null) {
      for (String k : reqs)
        if (getExpr(k) == null) {
          return k;
        }
    }
    return null;
  }

  // Return a new ClassAd that is one or more ads merged together.
  // Merging happens in order, so resulting ad will contain the
  // last value of a key.
  ClassAd merge(ClassAd... ads) {
    return new ClassAd(this).importAd(ads);
  }

  ClassAd importAd(ClassAd... ads) {
    // Insert attributes from second ad.
    if (ads != null) {
      for (ClassAd ad : ads)
        for (String s : ad) insert(s, ad.getExpr(s));
    }
    return this;
  }

  // Rename a field to another field. Does nothing if no key called from.
  public void rename(String from, String to) {
    Expr e = getExpr(from);
    if (e != null) {
      insert(to, e);
    }
  }

  // Return a ClassAd with strings trimmed and empty strings removed.
  ClassAd trim() {
    ClassAd ad = new ClassAd();

    for (String k : this) {
      Expr e = getExpr(k);

      if (e.type == Expr.STRING) {
        String s = e.stringValue().trim();
        if (!s.isEmpty()) {
          ad.insert(k, s);
        }
      } else {
        ad.insert(k, e);
      }
    }

    return ad;
  }

  // Iterator for the internal record.
  public Iterator<String> iterator() {
    return new Iterator<String>() {
      @SuppressWarnings("rawtypes")
      Iterator i = record.attributes();

      public boolean hasNext() {
        return i.hasNext();
      }

      public String next() {
        return i.next().toString();
      }

      public void remove() {
        i.remove();
      }
    };
  }

  // True if there was an error parsing this ad.
  public boolean error() {
    return error;
  }

  // Convert to a pretty string for printing.
  public String toString(boolean compact) {
    StringWriter sw = new StringWriter();
    int flags = ClassAdWriter.READABLE & ~ClassAdWriter.NO_ESCAPE_STRINGS;

    // Set up ad writer
    ClassAdWriter caw = new ClassAdWriter(sw);

    if (!compact) {
      caw.setFormatFlags(flags);
    }

    caw.print(record);
    caw.flush();
    caw.close();
    return sw.toString();
  }  // Constructors

  // Default to non-compact representation.
  public String toString() {
    return toString(false);
  }  // Create a ClassAd object wrapping a RecordExpr.

  // Convert to a compact byte string to send over a socket.
  public byte[] getBytes() {
    return record.toString().getBytes();
  }  // Create an empty ClassAd.
}
