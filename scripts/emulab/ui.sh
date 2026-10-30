#!/bin/bash
# ui.sh NAME [tap X Y | text STR | key KEYCODE]... : perform actions, then wait, screenshot to NAME.png and list UI nodes
. "${ANDROID_ENV:-$HOME/android-sdk/env.sh}"
name=$1; shift
while [ $# -gt 0 ]; do
  case $1 in
    tap) adb shell input tap $2 $3; shift 3; sleep 1;;
    text) adb shell input text "$2"; shift 2; sleep 0.5;;
    key) adb shell input keyevent $2; shift 2; sleep 0.5;;
    sleep) sleep $2; shift 2;;
    *) echo "bad action $1"; exit 1;;
  esac
done
sleep ${UI_WAIT:-3}
cd "${EMULAB_OUT:-.}"
adb exec-out screencap -p > $name.png
adb shell uiautomator dump /sdcard/ui.xml >/dev/null 2>&1 && adb exec-out cat /sdcard/ui.xml | python3 -c "
import sys,re
x=sys.stdin.read()
for m in re.finditer(r'<node[^>]*?text=\"([^\"]*)\"[^>]*?resource-id=\"([^\"]*)\"[^>]*?class=\"([^\"]*)\"[^>]*?content-desc=\"([^\"]*)\"[^>]*?clickable=\"(true|false)\"[^>]*?bounds=\"(\[[^\"]*\])\"', x):
    t,rid,cls,cd,cl,b=m.groups()
    if t or cd or cl=='true': print(cls.split('.')[-1], repr(t[:50]), repr(cd[:30]), rid[-30:], cl, b)
"
