#!/bin/bash
# Run data coordinator migrations
# run_migrations.sh [migrate/undo/redo] [numchanges]
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BASEDIR="$(dirname "${REALPATH}")/.."

CONFIG=${SODA_CONFIG:-$BASEDIR/../docs/onramp/services/soda2.conf} # TODO: Don't depend on soda2.conf.

JARFILE="$(ls -rt coordinator/target/scala-*/coordinator-assembly-*.jar 2>/dev/null | tail -n 1)"
if [ -z "$JARFILE" ] || find ./* -newer "$JARFILE" | egrep -q -v '(/target/)|(/bin/)'; then
    cd "$BASEDIR" || { echo 'Failed to change directories'; exit; }
    nice -n 19 sbt assembly
fi

COMMAND=${1:-migrate}
echo Running datacoordinator.primary.MigrateSchema "$COMMAND" "$2"...
ARGS=( $COMMAND $2 )
java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.primary.MigrateSchema "${ARGS[@]}"
