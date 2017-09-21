# force script to exit on failed command
set -e

PACKAGE=grakn-package

case $1 in
    start)
        mkdir ${PACKAGE}
        tar -xf ../grakn-dist/target/grakn-dist*.tar.gz --strip=1 -C ${PACKAGE}

        set +e
        nc -z localhost 4567
        PORT_IN_USE=$?
        set -e

        if [ $PORT_IN_USE -eq 0 ]; then
            >&2 echo "Port 4567 is in use. Maybe a Grakn server is already running?"
            exit 1
        fi

        ./${PACKAGE}/grakn server start
        ;;
    stop)
        ./${PACKAGE}/grakn server stop
        rm -rf ./${PACKAGE}
        ;;
    *)
        >&2 echo 'Valid commands are `start` and `stop`'
        exit 1
        ;;
esac