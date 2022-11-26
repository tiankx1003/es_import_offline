#!/bin/sh
cd "${0%/*}"/.. || exit

JAVA_HOME=/opt/module/jdk1.8.0_291

APP_HOME=$(
  cd "$(dirname "$0")/.." || exit
  pwd
)

APP_MAIN_CLASS=io.github.tiankx1003.ApplicationEntry

CLASSPATH=$APP_HOME/classes
CLASSPATH="$CLASSPATH":$APP_HOME/conf
for i in "$APP_HOME"/lib/*.jar; do
  CLASSPATH="$CLASSPATH":"$i"
done

JAVA_OPTS="-ms1024m -mx1024m -Xmn512m -Djava.awt.headless=true -XX:MaxPermSize=128m"

psid=0

check_pid() {
  java_ps=$($JAVA_HOME/bin/jps -l | grep $APP_MAIN_CLASS)

  if [ -n "$java_ps" ]; then
    psid=$(echo "$java_ps" | awk '{print $1}')
  else
    psid=0
  fi
}

start() {
  check_pid

  if [ $psid -ne 0 ]; then
    echo "================================"
    echo "warn: $APP_MAIN_CLASS already started! (pid=$psid)"
    echo "================================"
  else
    echo -n "Starting $APP_MAIN_CLASS ..."
    JAVA_CMD="nohup $JAVA_HOME/bin/java $JAVA_OPTS -classpath $CLASSPATH $APP_MAIN_CLASS > server.out 2>&1 &"
    sh -c "$JAVA_CMD"
    check_pid
    if [ $psid -ne 0 ]; then
      echo "(pid=$psid) [OK]"
    else
      echo "[Failed]"
    fi
  fi
}

stop() {
  check_pid

  if [ $psid -ne 0 ]; then
    echo -n "Stopping $APP_MAIN_CLASS ...(pid=$psid) "

    if sh -c "kill $psid"; then
      echo "[OK]"
    else
      echo "[Failed]"
    fi

    check_pid
    if [ $psid -ne 0 ]; then
      stop
    fi
  else
    echo "================================"
    echo "warn: $APP_MAIN_CLASS is not running"
    echo "================================"
  fi
}

status() {
  check_pid

  if [ $psid -ne 0 ]; then
    echo "$APP_MAIN_CLASS is running! (pid=$psid)"
  else
    echo "$APP_MAIN_CLASS is not running"
  fi
}

info() {
  echo "System Information:"
  echo "****************************"
  uname -a
  echo
  echo "JAVA_HOME=$JAVA_HOME"
  $JAVA_HOME/bin/java -version
  echo
  echo "APP_HOME=$APP_HOME"
  echo "APP_MAIN_CLASS=$APP_MAIN_CLASS"
  echo "****************************"
}

case "$1" in
'start')
  start
  ;;
'stop')
  stop
  ;;
'restart')
  stop
  start
  ;;
'status')
  status
  ;;
'info')
  info
  ;;
*)
  echo "Usage: $0 {start|stop|restart|status|info}"
  exit 1
  ;;
esac
