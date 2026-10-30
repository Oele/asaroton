# Emulator lab: Element X calls without a phone

A headless Android emulator running the release Element X APK, a headless Element Web driven by
Playwright, and a dev tessera with a lab LiveKit, all on one machine and all in user space.
This is how the sliding-sync `required_state` bugs that showed up as grey call tiles on phones
were reproduced and verified.

## One-time setup

- Android SDK under `~/android-sdk`: unpack an OpenJDK 21 tarball to `jdk/` and the
  command-line tools to `cmdline-tools/latest/`, then
  `sdkmanager platform-tools emulator "system-images;android-35;google_apis;x86_64"` and
  `avdmanager create avd -n ex -k "system-images;android-35;google_apis;x86_64" -d pixel_6`;
  add `hw.camera.back=emulated`, `hw.camera.front=emulated`, `hw.ramSize=4096` to the AVD's
  `config.ini`. Write an `env.sh` exporting `JAVA_HOME`, `ANDROID_HOME` and the tool paths;
  `ui.sh` sources it (`ANDROID_ENV` overrides the location).
- Element X: the `app-fdroid-x86_64-release-signed.apk` of the wanted version from the
  element-x-android GitHub releases (same code as the Play Store build minus push).
- Element Web: a release tarball served with `python3 -m http.server 8089`, its `config.json`
  pointing `default_server_config` at the dev server. A Python venv with
  `pip install playwright && playwright install chromium`.
- Dev tessera with `public_base_url = "http://localhost:8008"` and
  `[rtc.livekit] url = "ws://localhost:7890"` plus a `livekit-server` on 7890/7891 with the
  matching key. Both must be reachable as `localhost` inside the emulator and on the host:
  Element X allows cleartext HTTP only to localhost/10.0.2.2, and the Element Call WebView
  only accepts `ws://` towards localhost.

## Running

```sh
. ~/android-sdk/env.sh
QT_QPA_PLATFORM=offscreen emulator -avd ex -no-window -no-audio -gpu swiftshader_indirect \
    -no-boot-anim -no-snapshot -camera-back emulated -camera-front emulated &
adb wait-for-device; adb reverse tcp:8008 tcp:8008; adb reverse tcp:7890 tcp:7890
adb install -r app-fdroid-x86_64-release-signed.apk
python3 setup-room.py                 # users webdev/exdev, encrypted room, lab.json
~/.venvs/pw/bin/python webcall.py webdev callTest2026x '!room:localhost' 600 &   # web peer in the call
./ui.sh shot1                         # screenshot shot1.png + dump of the UI tree
./ui.sh shot2 tap 540 1882 text exdev key 4   # actions: tap X Y | text STR | key KEYCODE | sleep S
python3 sstest.py                     # what required_state carries on initial and incremental sync
```

Sign in by hand through `ui.sh` taps (the dump prints bounds); the password must be
alphanumeric because `adb shell input text` mangles punctuation. Dismiss the keyboard
(`key 4`) before tapping a button it covers. The emulator renders in software and is slow: wait
10–30 s after taps and for sync updates. `uiautomator` cannot see inside the Element Call
WebView, so read screenshots there. A member of a room created through the raw API cannot
join calls until power levels allow the call membership state events, which `setup-room.py`
sets like Element does on room creation.

## Testing the TURN relay

To imitate a client whose network blocks UDP, pass `ICE_DEBUG=1` and
`CHROME_ARGS='--force-webrtc-ip-handling-policy=disable_non_proxied_udp'` to `webcall.py`; the
console log then shows the ICE servers the client received, its candidates and the nominated
pair (`relay/udp/tls` when it went through TURN). Run the lab SFU with `tcp_port: 0` so ICE-TCP
cannot mask a broken relay, and list the SFU's own private address in `allow_peer_cidrs`, as
LiveKit refuses private relay peers by default. Chromium only uses TURN over TLS with a
certificate it trusts: generate one for a lab name (`turn.lab.test`), point the name at the lab
with `--host-resolver-rules="MAP turn.lab.test 127.0.0.1"` and trust it in a throwaway profile,
never in your own: `HOME=$lab/fakehome` with `certutil -d sql:$HOME/.pki/nssdb -A -t C,, -i cert.pem`
(`libnss3-tools`), plus `PLAYWRIGHT_BROWSERS_PATH` pointing at the real browser cache.
