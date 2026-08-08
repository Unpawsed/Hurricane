package haven;

import java.util.*;
import java.util.function.*;

/*
 * Central log of every reliable protocol message sent or received on a Session.
 * Fed from Session's single inbound (conncb.handle(PMessage)) and outbound
 * (queuemsg()) chokepoints, so this sees exactly what goes over the wire.
 *
 * PMessage keeps entirely separate read (rbuf) and write (wbuf) buffers.
 * Freshly-built outbound messages only have their write side populated
 * (addint32/addstring/... write to wbuf), while inbound messages arrive with
 * only their read side populated. Entries therefore capture plain (type, raw
 * bytes) pairs rather than PMessage instances, and reconstruct whichever side
 * is needed (read side to decode/display, write side to replay) on demand.
 */
public class NetPacketLog {
    public static final int maxlog = 2000;

    public enum Dir {IN, OUT}

    public static class Entry {
	public final Dir dir;
	public final double time;
	public final int type;
	public final byte[] raw;
	/* Client-assigned widget id, shared by both directions of a widget's
	 * traffic for its whole life; -1 if this message type carries none. */
	public final int wdgid;
	/* WDGMSG message name (e.g. "click", "act"); null for other types. */
	public final String wdgname;
	public final String label;

	Entry(Dir dir, double time, int type, byte[] raw, int wdgid, String wdgname, String label) {
	    this.dir = dir;
	    this.time = time;
	    this.type = type;
	    this.raw = raw;
	    this.wdgid = wdgid;
	    this.wdgname = wdgname;
	    this.label = label;
	}
    }

    public interface Filter {
	boolean test(Entry e);
    }

    public final Session sess;
    private final Deque<Entry> log = new ArrayDeque<>();
    private final List<Consumer<Entry>> listeners = new ArrayList<>();
    private final Map<String, Filter> filters = new LinkedHashMap<>();
    private final double epoch = Utils.rtime();
    private volatile boolean paused = false;
    /* Widget parentage as observed from NEWWDG/ADDWDG traffic, kept separate
     * from the bounded message log (and never pruned) so a subtree filter
     * still works even after the creation message itself has aged out of
     * the visible log. Ids can be reused after a DSTWDG, in which case a
     * later NEWWDG for the same id just overwrites its old parent. */
    private final Map<Integer, Integer> parentOf = new HashMap<>();
    private final Map<Integer, Set<Integer>> childrenOf = new HashMap<>();

    public NetPacketLog(Session sess) {
	this.sess = sess;
    }

    public boolean paused() {return(paused);}
    public void paused(boolean p) {paused = p;}

    public synchronized void record(Dir dir, PMessage msg) {
	if(paused)
	    return;
	/* Outbound messages are freshly written and only have wbuf populated;
	 * inbound messages arrive with only rbuf populated. Pull raw bytes
	 * from whichever side actually has the data. */
	byte[] raw = (dir == Dir.OUT) ? msg.fin() : new PMessage(msg).bytes();
	int wid = wdgid(msg.type, raw);
	int parent = parentid(msg.type, raw);
	if((wid >= 0) && (parent >= 0))
	    notewidget(wid, parent);
	Entry e = new Entry(dir, Utils.rtime() - epoch, msg.type, raw,
			     wid, wdgname(msg.type, raw), describe(msg.type, raw));
	log.addLast(e);
	while(log.size() > maxlog)
	    log.removeFirst();
	for(Consumer<Entry> l : listeners)
	    l.accept(e);
    }

    public synchronized List<Entry> entries() {
	return(new ArrayList<>(log));
    }

    public synchronized List<Entry> entries(Filter f) {
	if(f == null)
	    return(entries());
	List<Entry> ret = new ArrayList<>();
	for(Entry e : log) {
	    if(f.test(e))
		ret.add(e);
	}
	return(ret);
    }

    public synchronized void clear() {
	log.clear();
    }

    public void addListener(Consumer<Entry> l) {
	synchronized(listeners) {listeners.add(l);}
    }

    public void removeListener(Consumer<Entry> l) {
	synchronized(listeners) {listeners.remove(l);}
    }

    /* Named, toggleable filters that other dev tools can register against this log. */
    public void addFilter(String name, Filter f) {
	synchronized(filters) {filters.put(name, f);}
    }

    public void removeFilter(String name) {
	synchronized(filters) {filters.remove(name);}
    }

    public Map<String, Filter> filters() {
	synchronized(filters) {return(new LinkedHashMap<>(filters));}
    }

    /* Requeues a previously captured outbound message as a fresh send. */
    public void replay(Entry e) {
	if(e.dir != Dir.OUT)
	    throw(new IllegalArgumentException("can only replay outbound messages"));
	PMessage out = new PMessage(e.type);
	out.addbytes(e.raw);
	sess.queuemsg(out);
    }

    private static String rmsgname(int type) {
	switch(type) {
	case RMessage.RMSG_NEWWDG: return("NEWWDG");
	case RMessage.RMSG_WDGMSG: return("WDGMSG");
	case RMessage.RMSG_DSTWDG: return("DSTWDG");
	case RMessage.RMSG_MAPIV: return("MAPIV");
	case RMessage.RMSG_GLOBLOB: return("GLOBLOB");
	case RMessage.RMSG_RESID: return("RESID");
	case RMessage.RMSG_SESSKEY: return("SESSKEY");
	case RMessage.RMSG_FRAGMENT: return("FRAGMENT");
	case RMessage.RMSG_ADDWDG: return("ADDWDG");
	case RMessage.RMSG_WDGBAR: return("WDGBAR");
	case RMessage.RMSG_USERAGENT: return("USERAGENT");
	default: return("type " + type);
	}
    }

    private static int wdgid(int type, byte[] raw) {
	switch(type) {
	case RMessage.RMSG_WDGMSG:
	case RMessage.RMSG_NEWWDG:
	case RMessage.RMSG_DSTWDG:
	case RMessage.RMSG_ADDWDG:
	    try {
		return(new PMessage(type, raw).int32());
	    } catch(Exception ex) {
		return(-1);
	    }
	default:
	    return(-1);
	}
    }

    private static int parentid(int type, byte[] raw) {
	try {
	    PMessage cp = new PMessage(type, raw);
	    if(type == RMessage.RMSG_NEWWDG) {
		cp.int32();
		cp.string();
		return(cp.int32());
	    } else if(type == RMessage.RMSG_ADDWDG) {
		cp.int32();
		return(cp.int32());
	    }
	} catch(Exception ex) {
	}
	return(-1);
    }

    private void notewidget(int id, int parent) {
	Integer old = parentOf.put(id, parent);
	if((old != null) && (old.intValue() != parent)) {
	    Set<Integer> oldkids = childrenOf.get(old);
	    if(oldkids != null)
		oldkids.remove(id);
	}
	childrenOf.computeIfAbsent(parent, k -> new HashSet<>()).add(id);
    }

    /* id plus every widget transitively created under it, as observed from
     * NEWWDG/ADDWDG traffic. Descendants only -- does not walk up to
     * ancestors, since most widgets ultimately trace back to the shared UI
     * root and doing so would make the filter show nearly everything. */
    public synchronized Set<Integer> subtree(int id) {
	Set<Integer> seen = new HashSet<>();
	Deque<Integer> queue = new ArrayDeque<>();
	seen.add(id);
	queue.add(id);
	while(!queue.isEmpty()) {
	    int cur = queue.poll();
	    Set<Integer> kids = childrenOf.get(cur);
	    if(kids != null) {
		for(int kid : kids) {
		    if(seen.add(kid))
			queue.add(kid);
		}
	    }
	}
	return(seen);
    }

    private static String wdgname(int type, byte[] raw) {
	if(type != RMessage.RMSG_WDGMSG)
	    return(null);
	try {
	    PMessage cp = new PMessage(type, raw);
	    cp.int32();
	    return(cp.string());
	} catch(Exception ex) {
	    return(null);
	}
    }

    /* "tip" is a widget message name, not a protocol-level type, and its
     * argument shape is only a convention -- Widget.uimsg's default handler
     * (a plain string, or a resource id plus optional hover-only flag) is
     * what most widgets use, but some (IMeter, LayerMeter, ...) override
     * "tip" with their own args. This decodes the common case and leaves
     * anything else to the generic argument dump. */
    private static String tipsummary(Object[] args) {
	if(args.length == 0)
	    return("(empty)");
	Object tt = args[0];
	if(tt instanceof String)
	    return(String.format("text=\"%s\"", tt));
	if(tt instanceof Integer)
	    return(String.format("resource=#%s%s", tt, ((args.length > 1) ? (" hover-only=" + args[1]) : "")));
	return(null);
    }

    private static String describe(int type, byte[] raw) {
	String tn = rmsgname(type);
	try {
	    PMessage cp = new PMessage(type, raw);
	    if(type == RMessage.RMSG_WDGMSG) {
		int id = cp.int32();
		String name = cp.string();
		Object[] args = cp.list();
		if(name.equals("tip")) {
		    String tip = tipsummary(args);
		    if(tip != null)
			return(String.format("%s #%d tip %s", tn, id, tip));
		}
		return(String.format("%s #%d %s %s", tn, id, name, valuestr(args)));
	    } else if(type == RMessage.RMSG_NEWWDG) {
		int id = cp.int32();
		String wtype = cp.string();
		int parent = cp.int32();
		return(String.format("%s #%d %s parent=%d", tn, id, wtype, parent));
	    } else if(type == RMessage.RMSG_ADDWDG) {
		int id = cp.int32();
		int parent = cp.int32();
		return(String.format("%s #%d parent=%d", tn, id, parent));
	    } else if(type == RMessage.RMSG_DSTWDG) {
		return(String.format("%s #%d", tn, cp.int32()));
	    } else if(type == RMessage.RMSG_RESID) {
		int resid = cp.uint16();
		String resname = cp.string();
		int resver = cp.uint16();
		return(String.format("%s #%d %s v%d", tn, resid, resname, resver));
	    } else if(type == RMessage.RMSG_USERAGENT) {
		String key = cp.string();
		String val = cp.string();
		return(String.format("%s %s=%s", tn, key, val));
	    }
	} catch(Exception ex) {
	    /* Fall through to raw preview below. */
	}
	return(String.format("%s (%d bytes)", tn, raw.length));
    }

    private static final int rawPreviewCap = 256;

    /* Bounded, fast preview of raw bytes. MessageBuf.toString() (its default
     * hex dump) formats every byte with String.format, which is slow enough
     * per call that dumping a large payload -- GLOBLOB/MAPIV chunks can run
     * tens of KB -- noticeably stalls the UI thread. Utils.bprint.enc() is a
     * plain StringBuilder loop, and capping the length bounds the cost
     * regardless of how big the underlying message actually is. */
    private static String rawpreview(byte[] raw) {
	int n = Math.min(raw.length, rawPreviewCap);
	String s = Utils.bprint.enc(Arrays.copyOf(raw, n));
	if(raw.length > n)
	    s = s + "... (" + (raw.length - n) + " more bytes)";
	return(s);
    }

    private static final int valuestrMaxDepth = 8;

    /* Arrays.toString() doesn't recurse into nested arrays -- it just calls
     * Object.toString() on them, which for an array is the useless default
     * "[Ljava.lang.Object;@hash" form. Several widgets (e.g. LayerMeter's
     * "tip") nest Object[] inside Object[] (an ItemInfo.Raw-style encoding),
     * so format those recursively instead. Depth-capped defensively, not
     * because nesting this deep is expected. */
    private static String valuestr(Object o) {return(valuestr(o, 0));}

    private static String valuestr(Object o, int depth) {
	if(o == null)
	    return("null");
	if(o instanceof byte[])
	    return(rawpreview((byte[])o));
	if(o instanceof Object[]) {
	    if(depth >= valuestrMaxDepth)
		return("[...]");
	    Object[] arr = (Object[])o;
	    StringBuilder buf = new StringBuilder("[");
	    for(int i = 0; i < arr.length; i++) {
		if(i > 0)
		    buf.append(", ");
		buf.append(valuestr(arr[i], depth + 1));
	    }
	    return(buf.append(']').toString());
	}
	return(String.valueOf(o));
    }

    private static String typestr(Object o) {
	return((o == null) ? "null" : o.getClass().getSimpleName());
    }

    private static final int maxArgLines = 100;

    private static String indent(int depth) {
	StringBuilder buf = new StringBuilder();
	for(int i = 0; i < depth; i++)
	    buf.append("  ");
	return(buf.toString());
    }

    /* valuestr() is fine for the one-line log summary, but crams a nested
     * Object[] (e.g. LayerMeter's "tip" ItemInfo.Raw encoding) onto a single
     * line, which the detail panel then truncates -- so for the detail view,
     * walk nested arrays and give each leaf value its own indented line
     * instead. */
    private static void appendarg(List<String> lines, String label, Object val, int depth) {
	String pad = indent(depth);
	if(val instanceof Object[]) {
	    Object[] arr = (Object[])val;
	    lines.add(pad + label + " (" + arr.length + " items):");
	    if(depth >= valuestrMaxDepth) {
		lines.add(indent(depth + 1) + "...");
		return;
	    }
	    int n = Math.min(arr.length, maxArgLines);
	    for(int i = 0; i < n; i++)
		appendarg(lines, "[" + i + "]", arr[i], depth + 1);
	    if(arr.length > n)
		lines.add(indent(depth + 1) + "... (" + (arr.length - n) + " more)");
	} else {
	    lines.add(pad + label + " = " + valuestr(val) + "  (" + typestr(val) + ")");
	}
    }

    private static void args(List<String> lines, String label, Object[] args) {
	lines.add(String.format("%s (%d):", label, args.length));
	int n = Math.min(args.length, maxArgLines);
	for(int i = 0; i < n; i++)
	    appendarg(lines, "[" + i + "]", args[i], 1);
	if(args.length > n)
	    lines.add("  ... (" + (args.length - n) + " more)");
    }

    /* Full field-by-field breakdown of a single entry, for a UI detail/inspector
     * panel; describe() above is the condensed one-line version shown in the
     * log itself. Decodes every field the client protocol actually defines for
     * each RMSG type (cross-checked against RemoteUI/Session's own parsing),
     * not just the id/name subset describe() summarizes. */
    public static List<String> detail(Entry e) {
	List<String> lines = new ArrayList<>();
	lines.add("Direction: " + e.dir);
	lines.add(String.format("Time: %.3fs", e.time));
	lines.add("Type: " + rmsgname(e.type) + " (" + e.type + ")");
	lines.add("Size: " + e.raw.length + " bytes");
	lines.add("");
	try {
	    PMessage cp = new PMessage(e.type, e.raw);
	    switch(e.type) {
	    case RMessage.RMSG_WDGMSG: {
		lines.add("Widget id: #" + cp.int32());
		String name = cp.string();
		lines.add("Message: " + name);
		Object[] wargs = cp.list();
		if(name.equals("tip")) {
		    lines.add("");
		    lines.add("Tooltip:");
		    if(wargs.length == 0) {
			lines.add("  (empty)");
		    } else if(wargs[0] instanceof String) {
			lines.add("  Kind: plain text");
			lines.add("  Text: " + wargs[0]);
		    } else if(wargs[0] instanceof Integer) {
			lines.add("  Kind: resource reference");
			lines.add("  Resource id: #" + wargs[0]);
			if(wargs.length > 1)
			    lines.add("  Hover-only: " + wargs[1]);
		    } else {
			lines.add("  Kind: widget-specific (not the default text/resource shape)");
		    }
		    lines.add("");
		}
		args(lines, "Arguments", wargs);
		break;
	    }
	    case RMessage.RMSG_NEWWDG: {
		lines.add("Widget id: #" + cp.int32());
		lines.add("Widget type: " + cp.string());
		lines.add("Parent id: #" + cp.int32());
		args(lines, "Construction args", cp.list());
		args(lines, "Creation args", cp.list());
		break;
	    }
	    case RMessage.RMSG_ADDWDG: {
		lines.add("Widget id: #" + cp.int32());
		lines.add("Parent id: #" + cp.int32());
		args(lines, "Construction args", cp.list());
		break;
	    }
	    case RMessage.RMSG_DSTWDG: {
		lines.add("Widget id: #" + cp.int32());
		break;
	    }
	    case RMessage.RMSG_RESID: {
		lines.add("Resource id: #" + cp.uint16());
		lines.add("Resource name: " + cp.string());
		lines.add("Resource version: " + cp.uint16());
		break;
	    }
	    case RMessage.RMSG_USERAGENT: {
		lines.add(cp.string() + " = " + cp.string());
		break;
	    }
	    case RMessage.RMSG_WDGBAR: {
		List<Integer> deps = new ArrayList<>();
		while(!cp.eom()) {
		    int dep = cp.int32();
		    if(dep == -1)
			break;
		    deps.add(dep);
		}
		List<Integer> bars = deps;
		if(!cp.eom()) {
		    bars = new ArrayList<>();
		    while(!cp.eom()) {
			int bar = cp.int32();
			if(bar == -1)
			    break;
			bars.add(bar);
		    }
		}
		lines.add("Dependencies: " + deps);
		lines.add("Barriers: " + bars);
		break;
	    }
	    case RMessage.RMSG_SESSKEY: {
		lines.add("(session signing key material, " + e.raw.length + " bytes)");
		break;
	    }
	    default: {
		lines.add("(no field-level decoder for this type)");
		break;
	    }
	    }
	} catch(Exception ex) {
	    lines.add("(failed to decode: " + ex + ")");
	}
	return(lines);
    }
}
