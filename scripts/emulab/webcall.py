"""Headless Element Web peer for call tests: log in, open a room, start or join the Element Call
and stay in it, reporting the video elements it renders.

usage: webcall.py USER PASS ROOM_ID [SECONDS]
Environment: ELEMENT_WEB (default http://localhost:8089), EMULAB_OUT (screenshots and
web-console.log, default .). Needs `pip install playwright && playwright install chromium`.
"""
import os, sys, time, json
from playwright.sync_api import sync_playwright

user, pw, room_id = sys.argv[1], sys.argv[2], sys.argv[3]
stay = int(sys.argv[4]) if len(sys.argv) > 4 else 180
BASE = os.environ.get("ELEMENT_WEB", "http://localhost:8089")
OUT = os.environ.get("EMULAB_OUT", ".")
log = open(f"{OUT}/web-console.log", "a")

def note(msg):
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line, flush=True); log.write(line + "\n"); log.flush()

with sync_playwright() as p:
    # CHROME_ARGS adds flags, e.g. "--force-webrtc-ip-handling-policy=disable_non_proxied_udp" to
    # imitate a network that blocks UDP (the client must then relay through TURN over TLS).
    browser = p.chromium.launch(args=[
        "--use-fake-device-for-media-stream", "--use-fake-ui-for-media-stream",
        "--autoplay-policy=no-user-gesture-required",
        *os.environ.get("CHROME_ARGS", "").split(),
    ])
    ctx = browser.new_context(viewport={"width": 1280, "height": 900}, permissions=["camera", "microphone"])
    page = ctx.new_page()
    if os.environ.get("ICE_DEBUG"):
        # Log the ICE servers handed to each peer connection and every candidate error, which
        # shows whether a TURN relay is offered and why it fails.
        ctx.add_init_script("""(() => {
            const P = window.RTCPeerConnection; if (!P) return;
            const W = function(cfg, ...rest) {
                const pc = new P(cfg, ...rest);
                console.log('[icedebug] iceServers ' + JSON.stringify((cfg || {}).iceServers || []).slice(0, 800));
                pc.addEventListener('icecandidateerror', e => console.log(`[icedebug] error ${e.errorCode} ${e.url} ${e.errorText}`));
                pc.addEventListener('iceconnectionstatechange', () => console.log('[icedebug] state ' + pc.iceConnectionState));
                return pc;
            };
            W.prototype = P.prototype; Object.setPrototypeOf(W, P); window.RTCPeerConnection = W;
        })();""")
    page.on("console", lambda m: log.write(f"{time.strftime('%H:%M:%S')} [{m.type}] {m.text[:300]}\n"))
    page.goto(f"{BASE}/#/login")
    page.wait_for_selector("input[name=username], input[autocomplete=username]", timeout=60000)
    page.fill("input[name=username], input[autocomplete=username]", user)
    page.fill("input[type=password]", pw)
    page.keyboard.press("Enter")
    time.sleep(10)
    page.screenshot(path=f"{OUT}/web-postlogin.png")
    note("post-login url " + page.url)
    for label in ["Skip verification", "I'll verify later", "Verify later", "Skip", "Not now", "Dismiss"]:
        try:
            b = page.get_by_role("button", name=label).first
            if b.is_visible(timeout=1500):
                b.click(); note(f"clicked {label}"); time.sleep(2)
        except Exception:
            pass
    buttons = page.locator("button").all_inner_texts()
    note("buttons: " + " | ".join(t.strip() for t in buttons if t.strip())[:300])
    # Skip "verify this device" prompts if any by navigating straight to the room.
    page.goto(f"{BASE}/#/room/{room_id}")
    page.wait_for_selector("[data-testid=room-header-name], .mx_RoomHeader", timeout=60000)
    time.sleep(3)
    page.screenshot(path=f"{OUT}/web-room.png")
    # Start a video call: the header "Video call" button; if a call is active, it's "Join".
    btn = page.locator("button[aria-label='Video call'], button[aria-label='Join']").first
    btn.click(timeout=30000)
    note("clicked call button")
    # Possibly a menu offering Element Call vs legacy.
    try:
        page.get_by_text("Element Call", exact=False).first.click(timeout=4000)
        note("chose Element Call")
    except Exception:
        pass
    # The call lobby lives inside the widget iframe.
    t0 = time.time()
    joined = False
    while time.time() - t0 < 90 and not joined:
        for fr in page.frames:
            if fr != page.main_frame:
                try:
                    jb = fr.locator("button:has-text('Join call'), button:has-text('Join')").first
                    if jb.is_visible(timeout=1000):
                        jb.click(timeout=5000); note("joined call"); joined = True; break
                except Exception:
                    pass
        time.sleep(2)
    page.screenshot(path=f"{OUT}/web-call-start.png")
    t0 = time.time()
    while time.time() - t0 < stay:
        time.sleep(15)
        page.screenshot(path=f"{OUT}/web-call-{int(time.time()-t0):03d}.png")
        for fr in page.frames:
            if fr != page.main_frame:
                try:
                    n = fr.evaluate("Array.from(document.querySelectorAll('video')).map(v=>`${v.videoWidth}x${v.videoHeight}`).join(',')")
                    note(f"videos in frame {fr.url[:40]}: {n}")
                except Exception as e:
                    note(f"eval failed: {e}")
    note("leaving")
    browser.close()
