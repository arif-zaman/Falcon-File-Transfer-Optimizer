package transfer_protocol.util;

import java.util.Iterator;

// Isn't crazy that Java doesn't have one of these natively?!

class Range implements Iterable<Integer> {
  private boolean empty = true;
  private int start = 0, end = 0;
  private Range subrange = null;

  private Range() {
  }

  public Range(int i) {
    become(i, i);
  }

  private Range(int s, int e) {
    become(s, e);
  }

  public Range(String str) {
    become(parseRange(str));
  }

  private Range(Range r) {
    become(r);
  }

  // Parse a range from a string.
  public static Range parseRange(String str) {
    int s = -1, e = -1;
    Range r = new Range();

    for (String a : str.split(","))
      try {
        String[] b = a.split("-", 3);
        switch (b.length) {
          default:
            return null;
          case 1:
            s = e = Integer.parseInt(b[0]);
            break;
          case 2:
            s = Integer.parseInt(b[0]);
            e = Integer.parseInt(b[1]);
        }
        r.swallow(s, e);
      } catch (Exception ugh) {
        return null;
      }
    return r;
  }

  // Swallow (s,e) into this range so that this range includes
  // all of (s,e) in order and with minimal number of subranges.
  public Range swallow(int s, int e) {
    if (s > e) {
      return swallow(e, s);
    }
    if (empty) {
      return become(s, e);
    }

    // Eat as much of (s,e) as we can with this range.
    if (s < start) {  // Consider inserting before...
      if (e + 1 < start) {  // Independent. Shuffle down ranges...
        Range r = new Range(this);
        start = s;
        end = e;
        subrange = r;
        s = r.end;
      } else {  // Overlapping or adjacent! Easy mode.
        start = s;
        s = end;
      }
    }
    if (e > end) {  // Consider inserting after...
      // Let subrange have it, if we have one.
      if (subrange != null) {
        subrange.swallow(s, e);
      } else {
        subrange = new Range(s, e);
      }

      // Now see if we can swallow subrange.
      while (subrange != null && end + 1 >= subrange.start) {
        end = subrange.end;
        subrange = subrange.subrange;
      }
    }
    return this;
  }

  public Range swallow(int i) {
    return swallow(i, i);
  }

  // Swallow a range, subrange by subrange.
  public Range swallow(Range r) {
    if (empty) {
      return become(r);
    }
    while (r != null && !r.empty) {
      swallow(r.start, r.end);
      r = r.subrange;
    }
    return this;
  }

  // Check if a range contains an integer or range of integers.
  public boolean contains(int i) {
    return contains(i, i);
  }

  public boolean contains(int s, int e) {
    if (empty) {
      return false;
    }
    if (s > e) {
      int t = s;
      s = e;
      e = t;
    }
    return (start <= s && e <= end) ||
            (subrange != null && subrange.contains(s, e));
  }

  // Take on the characteristics of a given range. Return self.
  // Does nothing when passed null.
  private Range become(Range r) {
    if (r != null) {
      start = r.start;
      end = r.end;
      empty = r.empty;
      subrange = r.subrange;
    }
    return this;
  }

  // As above, but for integers.
  private Range become(int s, int e) {
    if (s > e) {
      become(e, s);
    }
    start = s;
    end = e;
    empty = false;
    subrange = null;
    return this;
  }

  public boolean isEmpty() {
    return empty;
  }

  // Get the size of the range.
  public int size() {
    if (empty) {
      return 0;
    }
    if (subrange == null) {
      return end - start + 1 + subrange.size();
    }
    return end - start + 1;
  }

  public int min() {
    return start;
  }

  public int max() {
    return (subrange == null) ? end : subrange.max();
  }

  public boolean isContiguous() {
    return subrange == null;
  }

  public boolean isNumber() {
    return subrange == null && start == end;
  }

  // Return a string representation of this range. This string
  // can be parsed back into a range.
  public String toString() {
    if (empty) {
      return "";
    }
    return ((start == end) ? "" + start : start + "-" + end) +
            ((subrange != null) ? "," + subrange : "");
  }

  // Get an iterator for this range.
  public Iterator<Integer> iterator() {
    return new Iterator<Integer>() {
      Range r = Range.this;
      int i = r.start;

      private void nextSubrange() {
        if (r == null) {
          return;
        }
        r = r.subrange;
        if (r != null) {
          i = r.start;
        }
      }

      public boolean hasNext() {
        return i <= r.end || r.subrange != null;
      }

      public Integer next() {
        if (r == null) {
          return null;
        }
        if (r.empty) {
          return null;
        }
        if (i <= r.end) {
          return new Integer(i++);
        }
        nextSubrange();
        return next();
      }

      public void remove() {
      }
    };
  }
}
