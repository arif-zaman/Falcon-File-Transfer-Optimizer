package transfer_protocol.util;


// Special ad for module information.
//
// The following parameters are required:
//   name - The name of the module. Will be used to generate handle
//          (see StorkUtil.normalize()) if handle isn't present.
//   protocols - A comma-separated list of supported protocols.
//
// The following are optional recognized parameters:
//   handle - A normalized name for this module that can be used to
//            force jobs to be passed to this module. Will be
//            automatically generated from name if not present.
//   author - The name of the module's author. That's you!
//   version - A string indicating module version.
//   description - A short, friendly description of the module.
//   accepts - The expected format of job control information.
//             Valid options: classads, none (default)
//   opt_params - A comma-separated list of optional parameters the
//                module allows in its job ads.
//   req_params - Like opt_params, but the module requires these.

public class ModuleInfoAd extends ClassAd {
  // Fields that are okay to have in the ad.
  static final String[] fields = {
          "name", "handle", "author", "email", "version", "description",
          "protocols", "accepts", "opt_params", "req_params"};
  // Possible values for accept. Keep the last element null.
  // Why? Shitty inline array search algorithm below. ;)
  static final String[] accepts_vals = {"none", "classads", null};
  public final String name, full_name, handle, author, version,
          description, accepts;
  public final String[] protocols, opt_params, req_params;

  // Because we trim the ad at the beginning, all strings in this
  // ad are guaranteed to be trimmed and not empty. TODO: Sanitization.
  public ModuleInfoAd(ClassAd ad) throws Exception {
    super(ad.filter(fields).trim());

    // Check for required protocols.
    String p = require("name", "protocols");

    if (p != null) {
      throw new Exception("missing parameter: " + p);
    }

    name = get("name");

    protocols = StorkUtil.splitCSV(get("protocols"));
    if (protocols.length == 0) {
      throw new Exception("no supported protocols listed");
    }
    insert("protocols", StorkUtil.joinCSV((Object[]) protocols));

    handle = StorkUtil.normalize(get("handle", name));
    if (handle.isEmpty()) {
      throw new Exception("invalid handle: " + handle);
    }
    insert("handle", handle);

    version = get("version", "");
    full_name = (name + " " + version).trim();
    author = get("author");
    description = get("description");

    accepts = get("accepts", "none").toLowerCase();
    for (String s : accepts_vals) {
      if (s == null) {
        throw new Exception("invalid value for \"accepts\": " + accepts);
      }
      if (s.equals(accepts)) {
        break;
      }
    }
    insert("accepts", accepts);

    opt_params = StorkUtil.splitCSV(get("opt_params"));
    insert("opt_params", StorkUtil.joinCSV((Object[]) opt_params));

    req_params = StorkUtil.splitCSV(get("req_params"));
    insert("req_params", StorkUtil.joinCSV((Object[]) req_params));
  }

  // Insert into ad, automatically trimming. If v is null or
  // empty when trimmed, removes key from ad.
  public ClassAd insert(String k, String v) {
    if (v != null && (v = v.trim()).isEmpty()) {
      v = null;
    }
    return super.insert(k, v);
  }
}
