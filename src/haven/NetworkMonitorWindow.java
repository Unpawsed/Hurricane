package haven;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;

/*
 * Developer window showing every reliable protocol message sent/received on the
 * current session, backed by Session.netlog (NetPacketLog). Supports live text
 * filtering, direction toggles, and replaying a previously sent (OUT) message.
 */
public class NetworkMonitorWindow extends Window {
    private static final Color incol = new Color(180, 255, 180);
    private static final Color outcol = new Color(180, 210, 255);
    private static final Text.Foundry rowf = new Text.Foundry(Text.mono, 12);
    private static final Text.Foundry detailf = new Text.Foundry(Text.mono, 12);
    private static final int detailLineh = UI.scale(15);
    private static final int detailMaxChars = 60;
    /* Packet-driven rebuilds (new traffic arriving) are throttled to this
     * interval so a busy session doesn't tear down/rebuild the visible row
     * widgets dozens of times a second; user-driven changes (typing in the
     * filter box, toggling a checkbox, Clear) still apply immediately. */
    private static final double rebuildInterval = 0.15;
    /* High-frequency, low-value WDGMSG names (tooltip queries, periodic
     * widget state pushes) that dominate the log without saying much. */
    private static final java.util.Set<String> noisy = new java.util.HashSet<>(java.util.Arrays.asList("set", "tip", "glut"));

    private final Session sess;
    private final PacketList list;
    private final Scrollport detail;
    private final TextEntry filter;
    private final CheckBox showIn, showOut, follow, hideNoisy;
    private final Label status;
    private final List<NetPacketLog.Entry> filtered = new ArrayList<>();
    private final java.util.function.Consumer<NetPacketLog.Entry> onpacket = e -> dirty = true;
    private volatile boolean dirty = true;
    private double lastRebuild = -1;
    private NetPacketLog.Entry shownDetail = null;
    private String shownStatus = null;

    public NetworkMonitorWindow(Session sess) {
	super(UI.scale(1190, 430), "Network Monitor");
	this.sess = sess;

	Widget prev;
	prev = add(new Label("Filter"), UI.scale(0, 6));
	filter = add(new TextEntry(UI.scale(220), "") {
		protected void changed() {
		    dirty = true;
		    super.changed();
		}
	    }, prev.pos("ur").adds(5, -3));
	filter.tooltip = RichText.render("Substring match against the decoded message text.\n$col[185,185,185]{Type $col[255,255,255]{id:<n>} for just widget #n, or $col[255,255,255]{tree:<n>} for #n and everything created under it -- or click a row and hit \"Same Widget\" for the latter.}", UI.scale(300));
	prev = add(showIn = new CheckBox("In") {
		{a = true;}
		public void changed(boolean val) {
		    dirty = true;
		    super.changed(val);
		}
	    }, filter.pos("ur").adds(15, 3));
	prev = add(showOut = new CheckBox("Out") {
		{a = true;}
		public void changed(boolean val) {
		    dirty = true;
		    super.changed(val);
		}
	    }, prev.pos("ur").adds(5, 0));
	prev = add(follow = new CheckBox("Follow") {
		{a = true;}
	    }, prev.pos("ur").adds(5, 0));
	prev = add(hideNoisy = new CheckBox("Hide noisy") {
		{a = true;}
		public void changed(boolean val) {
		    dirty = true;
		    super.changed(val);
		}
	    }, prev.pos("ur").adds(5, 0));
	hideNoisy.tooltip = RichText.render("Hides WDGMSG traffic that's almost always noise: tooltip queries (tip), periodic widget state pushes (set), and gauge updates (glut).", UI.scale(300));

	list = add(new PacketList(UI.scale(740, 340)), UI.scale(0, 35));

	add(new Label("Details (click a message)"), list.pos("ur").adds(10, 0));
	detail = add(new Scrollport(UI.scale(420, 320)), list.pos("ur").adds(10, 20));

	sess.netlog.addListener(onpacket);

	prev = add(new Button(UI.scale(120), "Replay Selected", false) {
		public void click() {
		    NetPacketLog.Entry sel = list.sel;
		    if((sel != null) && (sel.dir == NetPacketLog.Dir.OUT))
			sess.netlog.replay(sel);
		}
	    }, list.pos("bl").adds(0, 8));
	prev = add(new Button(UI.scale(80), "Clear", false) {
		public void click() {
		    sess.netlog.clear();
		    dirty = true;
		}
	    }, prev.pos("ur").adds(5, 0));
	prev = add(new Button(UI.scale(100), "Same Widget", false) {
		public void click() {
		    NetPacketLog.Entry sel = list.sel;
		    if((sel != null) && (sel.wdgid >= 0)) {
			filter.settext("tree:" + sel.wdgid);
			dirty = true;
		    }
		}
	    }, prev.pos("ur").adds(5, 0));
	status = add(new Label(""), prev.pos("ur").adds(15, 5));

	pack();
    }

    /* Detaching from the log matters for more than tidiness: the listener is
     * what makes NetPacketLog record at all, so leaving it attached would keep
     * the log (and this window, which the closure references) alive and
     * collecting for the rest of the session. */
    public void destroy() {
	sess.netlog.removeListener(onpacket);
	Utils.setprefc("wndc-netmon", this.c);
	super.destroy();
    }

    public void tick(double dt) {
	if(dirty) {
	    double now = Utils.rtime();
	    if((lastRebuild < 0) || ((now - lastRebuild) >= rebuildInterval)) {
		rebuild();
		lastRebuild = now;
	    }
	}
	/* Label.settext re-renders unconditionally in this fork (its
	 * short-circuit is commented out), so only call it on real changes --
	 * otherwise this is a font render plus texture upload every frame. */
	String st = filtered.size() + " / " + sess.netlog.size() + " shown";
	if(!st.equals(shownStatus))
	    status.settext(shownStatus = st);
	if(list.sel != shownDetail)
	    rebuilddetail();
	super.tick(dt);
    }

    private void rebuilddetail() {
	shownDetail = list.sel;
	Widget next;
	for(Widget w = detail.cont.child; w != null; w = next) {
	    next = w.next;
	    w.destroy();
	}
	if(shownDetail == null)
	    return;
	int y = 0;
	for(String line : NetPacketLog.detail(shownDetail)) {
	    if(line.length() > detailMaxChars)
		line = line.substring(0, detailMaxChars - 3) + "...";
	    if(!line.isEmpty())
		detail.cont.add(new DetailLine(line), Coord.of(0, y));
	    y += detailLineh;
	}
    }

    /* A plain Label renders its texture in the constructor, so rebuilding a
     * few hundred of them on a single click would mean that many font renders
     * and texture uploads in one frame. Scrollport skips drawing off-screen
     * children, so deferring the render to draw() means only the lines
     * actually scrolled into view ever cost anything. */
    private static class DetailLine extends Widget {
	private final String text;
	private Tex tex = null;

	DetailLine(String text) {
	    super(Coord.of(UI.scale(420), detailLineh));
	    this.text = text;
	}

	public void draw(GOut g) {
	    try {
		if(tex == null)
		    tex = detailf.render(text).tex();
		g.image(tex, Coord.z);
	    } catch(Loading l) {
		/* Font not ready yet; try again next frame. */
	    }
	    super.draw(g);
	}

	public void dispose() {
	    if(tex != null) {
		tex.dispose();
		tex = null;
	    }
	    super.dispose();
	}
    }

    private void rebuild() {
	dirty = false;
	String raw = filter.text().trim();
	Integer idfilter = null;
	java.util.Set<Integer> treefilter = null;
	String needle = null;
	if(raw.toLowerCase().startsWith("tree:")) {
	    try {
		treefilter = sess.netlog.subtree(Integer.parseInt(raw.substring(5).trim()));
	    } catch(NumberFormatException ex) {
		/* Not a valid id: filter falls through to showing nothing rather than everything. */
	    }
	} else if(raw.toLowerCase().startsWith("id:")) {
	    try {
		idfilter = Integer.parseInt(raw.substring(3).trim());
	    } catch(NumberFormatException ex) {
		/* Not a valid id: filter falls through to showing nothing rather than everything. */
	    }
	} else if(!raw.isEmpty()) {
	    needle = raw.toLowerCase();
	}
	/* Once a filter targets one widget specifically, "noisy" message names
	 * aren't noise anymore -- they're most of what you're there to see. */
	boolean targeted = (treefilter != null) || (idfilter != null);
	filtered.clear();
	for(NetPacketLog.Entry e : sess.netlog.entries()) {
	    if((e.dir == NetPacketLog.Dir.IN) && !showIn.a)
		continue;
	    if((e.dir == NetPacketLog.Dir.OUT) && !showOut.a)
		continue;
	    if(!targeted && hideNoisy.a && (e.wdgname != null) && noisy.contains(e.wdgname))
		continue;
	    if(treefilter != null) {
		if(!treefilter.contains(e.wdgid))
		    continue;
	    } else if(idfilter != null) {
		if(e.wdgid != idfilter)
		    continue;
	    } else if((needle != null) && !e.label().toLowerCase().contains(needle)) {
		continue;
	    }
	    filtered.add(e);
	}
	/* update() recomputes SListBox's row count/maxy/clamped cury for the
	 * filtered list we just rebuilt. Without this, scrollval() below sets
	 * cury to an out-of-range sentinel that only gets clamped back into
	 * bounds the next time update() sees the item count change  -- and a
	 * rebuild that doesn't change filtered.size() (e.g. a packet arrived
	 * but got filtered out) leaves the row window pointing past the end
	 * of the list, which destroys every visible row and shows nothing
	 * until some later rebuild happens to change the count. */
	list.update();
	if(follow.a)
	    /* scrollmax() can be negative (fewer rows than fit the viewport,
	     * or none at all after Clear) -- clamp to 0 ourselves. update()
	     * only clamps negative cury back to 0 as a side effect of seeing
	     * the item count change, and won't run again before the next
	     * natural tick, which otherwise computes a negative row offset
	     * and indexes the (possibly empty) item list with it, throwing. */
	    list.scrollval(Math.max(0, list.scrollmax()));
    }

    public void wdgmsg(Widget sender, String msg, Object... args) {
	/* Destroy rather than hide: hiding would leave the log listener
	 * attached, so the monitor would keep recording every packet for the
	 * rest of the session after being opened once. ":netmon" rebuilds it. */
	if((sender == this) && (msg == "close"))
	    destroy();
	else
	    super.wdgmsg(sender, msg, args);
    }

    private class PacketList extends SListBox<NetPacketLog.Entry, PacketList.Row> {
	PacketList(Coord sz) {
	    super(sz, UI.scale(18));
	}

	protected List<NetPacketLog.Entry> items() {
	    return(filtered);
	}

	protected Row makeitem(NetPacketLog.Entry item, int idx, Coord sz) {
	    return(new Row(this, sz, item));
	}

	class Row extends SListWidget.ItemWidget<NetPacketLog.Entry> {
	    private Tex tex = null;

	    Row(PacketList list, Coord sz, NetPacketLog.Entry item) {
		super(list, sz, item);
	    }

	    private String text() {
		return(String.format("%8.3f %-3s %s", item.time, (item.dir == NetPacketLog.Dir.OUT) ? "OUT" : "IN", item.label()));
	    }

	    public void draw(GOut g) {
		if(list.sel == item) {
		    g.chcolor(255, 255, 255, 40);
		    g.frect2(Coord.z, sz);
		    g.chcolor();
		}
		try {
		    /* Rendered lazily (not in the constructor) so a transient
		     * Loading from the font/glyph cache can't abort SListBox's
		     * row rebuild mid-way and leave it with no rows at all;
		     * we just retry on the next frame instead. */
		    if(tex == null) {
			Color col = (item.dir == NetPacketLog.Dir.OUT) ? outcol : incol;
			tex = rowf.render(text(), col).tex();
		    }
		    g.image(tex, Coord.of(UI.scale(4), (sz.y - tex.sz().y) / 2));
		} catch(Loading l) {
		    /* Font not ready yet; try again next frame. */
		}
		super.draw(g);
	    }

	    public void dispose() {
		if(tex != null)
		    tex.dispose();
		super.dispose();
	    }
	}
    }
}
