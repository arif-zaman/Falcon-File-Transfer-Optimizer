package transfer_protocol.util;


// A special ClassAd which contains a response code which can be
// either "error", "success", or "notice". It can also optionally
// contain a message or other fields.

class ResponseAd extends ClassAd {
  private static final String
          ERROR = "Error", SUCCESS = "Success", NOTICE = "Notice";
  private String response = ERROR;
  private String message = null;

  // Constructors
  public ResponseAd(String code, String message) {
    setResponse(code);
    setMessage(message);
  }

  public ResponseAd(String code) {
    this(code, null);
  }

  public ResponseAd(ClassAd ad) {
    this.importAd(ad);
    if (ad instanceof ResponseAd) {
      return;
    }
    setResponse(ad.get("response"));
    setMessage(ad.get("message"));
  }

  // Test if a ClassAd is a response ad.
  public static boolean is(ClassAd ad) {
    return ad.has("response");
  }

  // Easy accessors for response code and message;
  public String response() {
    return response;
  }

  public boolean success() {
    return response == SUCCESS;
  }

  public boolean error() {
    return response == ERROR;
  }

  @Override
  public String toString(boolean compact) {
    return super.toString(compact);
  }

  public boolean notice() {
    return response == NOTICE;
  }

  public String message() {
    return message;
  }

  // Set the response code in a safe way.
  public void setResponse(String c) {
    if (c == null || c.equalsIgnoreCase(ERROR)) {
      response = ERROR;
    } else if (c.equalsIgnoreCase(SUCCESS)) {
      response = SUCCESS;
    } else if (c.equalsIgnoreCase(NOTICE)) {
      response = NOTICE;
    } else {
      response = ERROR;
    }
    insert("response", response);
  }

  public void setMessage(String m) {
    if (m == null) {
      remove("message");
    } else {
      insert("message", m);
    }
    message = m;
  }

  public void set(String c, String m) {
    setResponse(c);
    setMessage(m);
  }

  // Return a string showing the response and message.
  public String toDisplayString() {
    return response + ((message != null) ? ": " + message : "");
  }
}
